package io.github.rvbcrs.wallmode

import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.NetworkRequest
import android.os.BatteryManager
import android.os.IBinder
import android.os.PowerManager
import android.util.Log
import android.webkit.WebView
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch

/**
 * Background service that owns the [MqttManager] lifecycle. It survives
 * independently of [MainActivity], so MQTT stays connected even when the OS
 * kills or recreates the activity (e.g. while the screen is off).
 *
 * Communication with the activity is through [MqttBridge]:
 *  - Activity pushes UI state → bridge [MqttBridge.dashboardState] → service merges
 *    with live device readings → broker
 *  - Broker sends commands → service → bridge [MqttBridge.commands] → activity
 *
 * Device-level telemetry (battery, screen, network, illuminance) is read directly
 * by this service so it stays accurate even when the activity is not running.
 */
class MqttService : Service() {

    private lateinit var prefs: KioskPreferences
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var mqttManager: MqttManager? = null
    private var periodicStateJob: Job? = null
    private var bridgeCollectionJob: Job? = null
    private var networkCallbackRegistered = false

    private val connectivityManager: ConnectivityManager by lazy {
        getSystemService(Context.CONNECTIVITY_SERVICE) as ConnectivityManager
    }
    private val powerManager: PowerManager by lazy {
        getSystemService(Context.POWER_SERVICE) as PowerManager
    }

    private val networkCallback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) {
            mqttManager?.retryInitialConnection()
            publishNow()
        }
        override fun onLost(network: Network) {
            publishNow()
        }
    }

    override fun onCreate() {
        super.onCreate()
        prefs = KioskPreferences(this)
        registerNetworkCallback()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_STOP) {
            stopMqttManager(removeDiscovery = true)
            stopSelf()
            return START_NOT_STICKY
        }
        refreshMqttManager()
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        unregisterNetworkCallback()
        stopMqttManager(removeDiscovery = false)
        serviceScope.cancel()
        super.onDestroy()
    }

    private fun registerNetworkCallback() {
        if (networkCallbackRegistered) return
        runCatching {
            connectivityManager.registerNetworkCallback(
                NetworkRequest.Builder()
                    .addCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
                    .build(),
                networkCallback
            )
            networkCallbackRegistered = true
        }
    }

    private fun unregisterNetworkCallback() {
        if (!networkCallbackRegistered) return
        runCatching { connectivityManager.unregisterNetworkCallback(networkCallback) }
        networkCallbackRegistered = false
    }

    private fun refreshMqttManager() {
        val settings = prefs.load()
        val storedPassword = prefs.mqttPassword()

        if (!settings.mqttEnabled) {
            stopMqttManager(removeDiscovery = true)
            prefs.recordMqttState("disabled")
            return
        }
        if (!MqttContract.isValidBroker(settings.mqttBrokerHost, settings.mqttBrokerPort) ||
            (storedPassword.isNotEmpty() && settings.mqttUsername.isBlank())
        ) {
            stopMqttManager(removeDiscovery = true)
            prefs.recordMqttState("configuration error")
            return
        }

        mqttManager?.takeIf { it.matches(settings, storedPassword) }?.let { existing ->
            existing.retryInitialConnection()
            existing.publishState(buildFullState(MqttBridge.dashboardState.value))
            return
        }

        stopMqttManager(removeDiscovery = true)

        val manager = runCatching {
            MqttManager(
                deviceKey = prefs.mqttDeviceKey(),
                settings = settings,
                password = storedPassword,
                appVersion = currentVersionName(),
                onCommand = { cmd -> MqttBridge.emitCommand(cmd) },
                onConnectionState = prefs::recordMqttState,
                onStateRequested = { publishNow() }
            )
        }.getOrElse {
            prefs.recordMqttState("configuration error")
            Log.w(TAG, "MQTT setup failed", it)
            return
        }
        mqttManager = manager
        manager.start()
        manager.publishState(buildFullState(MqttBridge.dashboardState.value))

        bridgeCollectionJob?.cancel()
        bridgeCollectionJob = collectBridgeFlows(manager)
        startPeriodicStatePublish(manager)
    }

    private fun collectBridgeFlows(manager: MqttManager): Job {
        return serviceScope.launch {
            launch {
                MqttBridge.dashboardState.distinctUntilChanged().collect { dashboard ->
                    manager.publishState(buildFullState(dashboard))
                }
            }
            launch {
                IlluminanceState.publishRequests.collect {
                    manager.publishState(buildFullState(MqttBridge.dashboardState.value))
                }
            }
            launch {
                MqttBridge.motionEvents.collect {
                    manager.publishMotion()
                }
            }
            launch {
                MqttBridge.actionResponses.collect { (cardId, actionId) ->
                    manager.publishActionResponse(cardId, actionId)
                }
            }
        }
    }

    private fun startPeriodicStatePublish(manager: MqttManager) {
        periodicStateJob?.cancel()
        periodicStateJob = serviceScope.launch {
            while (isActive) {
                delay(MQTT_STATE_INTERVAL_MS)
                manager.publishState(buildFullState(MqttBridge.dashboardState.value))
            }
        }
    }

    private fun publishNow() {
        mqttManager?.publishState(buildFullState(MqttBridge.dashboardState.value))
    }

    private fun buildFullState(dashboard: DashboardState?): WallModeMqttState {
        val battery = registerReceiver(null, IntentFilter(Intent.ACTION_BATTERY_CHANGED))
        val level = battery?.getIntExtra(BatteryManager.EXTRA_LEVEL, -1) ?: -1
        val scale = battery?.getIntExtra(BatteryManager.EXTRA_SCALE, -1) ?: -1
        val batteryPercent = if (level >= 0 && scale > 0) {
            (level * 100 / scale).coerceIn(0, 100)
        } else {
            0
        }
        val batteryStatus = battery?.getIntExtra(BatteryManager.EXTRA_STATUS, -1) ?: -1
        val charging = batteryStatus == BatteryManager.BATTERY_STATUS_CHARGING ||
            batteryStatus == BatteryManager.BATTERY_STATUS_FULL

        val networkConnected = connectivityManager.activeNetwork?.let { network ->
            connectivityManager.getNetworkCapabilities(network)
                ?.hasCapability(NetworkCapabilities.NET_CAPABILITY_INTERNET)
        } == true

        val screenOn = powerManager.isInteractive

        val settings = prefs.load()
        val displayMode = dashboard?.displayMode ?: MqttContract.DISPLAY_DASHBOARD
        val dashboardProblem = dashboard?.dashboardProblem ?: true
        val health = when {
            !networkConnected -> "offline"
            dashboardProblem -> "recovering"
            else -> "ok"
        }

        return WallModeMqttState(
            batteryPercent = batteryPercent,
            charging = charging,
            screenOn = screenOn,
            screensaverActive = dashboard?.screensaverActive ?: false,
            dashboardProblem = dashboardProblem,
            currentPage = dashboard?.currentPage ?: "-",
            health = health,
            webViewVersion = currentWebViewVersion(),
            appVersion = currentVersionName(),
            ambientBrightness = dashboard?.ambientBrightness ?: settings.ambientBrightnessPercent,
            ambientTimeout = dashboard?.ambientTimeout ?: settings.ambientDimAfterSeconds,
            ambientScene = dashboard?.ambientScene
                ?: MqttContract.ambientSceneName(settings.ambientScene),
            activeProfile = dashboard?.activeProfile ?: MqttContract.profileName(null),
            profileScheduleEnabled = dashboard?.profileScheduleEnabled
                ?: settings.scheduleProfilesEnabled,
            browserEngine = dashboard?.browserEngine ?: if (settings.browserEngine == BrowserEngine.WEBVIEW) {
                MqttContract.ENGINE_WEBVIEW
            } else {
                MqttContract.ENGINE_CHROMIUM
            },
            displayMode = displayMode,
            networkConnected = networkConnected,
            takeoverActive = dashboard?.takeoverActive ?: false,
            takeoverId = dashboard?.takeoverId.orEmpty(),
            takeoverPriority = dashboard?.takeoverPriority ?: 0,
            illuminanceLux = if (settings.illuminanceReportingEnabled) {
                IlluminanceState.currentLux()
            } else {
                null
            }
        )
    }

    private fun stopMqttManager(removeDiscovery: Boolean) {
        periodicStateJob?.cancel()
        periodicStateJob = null
        bridgeCollectionJob?.cancel()
        bridgeCollectionJob = null
        mqttManager?.stop(removeDiscovery = removeDiscovery)
        mqttManager = null
    }

    private fun currentVersionName(): String {
        return runCatching {
            packageManager.getPackageInfo(packageName, 0).versionName ?: "0.0.0"
        }.getOrDefault("0.0.0")
    }

    private fun currentWebViewVersion(): String {
        return runCatching {
            WebView.getCurrentWebViewPackage()?.versionName ?: "unknown"
        }.getOrDefault("unknown")
    }

    companion object {
        private const val TAG = "MqttService"
        private const val MQTT_STATE_INTERVAL_MS = 60_000L
        const val ACTION_STOP = "io.github.rvbcrs.wallmode.action.MQTT_STOP"

        fun start(context: Context) {
            context.startService(Intent(context, MqttService::class.java))
        }

        fun stop(context: Context) {
            context.startService(
                Intent(context, MqttService::class.java).apply { action = ACTION_STOP }
            )
        }
    }
}
