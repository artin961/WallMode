package io.github.rvbcrs.wallmode

object AppVisibility {
    @Volatile var settingsOpen = false
    @Volatile var userExited = false
}