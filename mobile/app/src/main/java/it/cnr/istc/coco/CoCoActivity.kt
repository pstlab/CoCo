package it.cnr.istc.coco

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue

class CoCoActivity : ComponentActivity() {

    private val viewModel: CoCoViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        setContent {
            val state by viewModel.appState.collectAsState()
            when (state) {
                is CoCoViewModel.State.Checking -> {
                    // Show a loading indicator
                }

                is CoCoViewModel.State.Authenticated -> {
                    // Show the main app content
                }

                is CoCoViewModel.State.Unauthenticated -> {
                    // Show the login screen
                }
            }
        }
    }
}