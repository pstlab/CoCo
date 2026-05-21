package it.cnr.istc.coco

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.viewModels

class CoCoActivity : ComponentActivity() {

    private val viewModel: CoCoViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
    }
}