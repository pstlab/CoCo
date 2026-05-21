package it.cnr.istc.coco

import android.app.Application
import android.content.Context
import androidx.lifecycle.AndroidViewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow

class CoCoViewModel(application: Application) : AndroidViewModel(application) {

    private val prefs = application.getSharedPreferences("secure", Context.MODE_PRIVATE)

    sealed class State {
        object Checking : State()
        object Authenticated : State()
        object Unauthenticated : State()
    }

    private val _appState = MutableStateFlow<State>(State.Checking)
    val appState: StateFlow<State> = _appState

    init {
        val token = prefs.getString("access_token", null)
        _appState.value = if (token != null) State.Authenticated else State.Unauthenticated
    }
}