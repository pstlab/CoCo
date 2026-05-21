package it.cnr.istc.coco

import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.POST
import retrofit2.Retrofit

data class Credentials(val username: String, val password: String)
data class TokenResponse(val access_token: String, val refresh_token: String, val token_type: String)

interface ApiService {
    @POST("login")
    suspend fun login(@Body request: Credentials): TokenResponse
}

object ApiClient {
    private const val BASE_URL = "https://coco.cnr.it/api/"

    val apiService: ApiService by lazy {
        Retrofit.Builder().baseUrl(BASE_URL).addConverterFactory(GsonConverterFactory.create()).build()
            .create(ApiService::class.java)
    }
}

class CoCo {
}