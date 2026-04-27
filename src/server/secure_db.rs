use crate::db::DatabaseError;
use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use futures::TryStreamExt;
use mongodb::bson::doc;
use mongodb::options::IndexOptions;
use mongodb::{Client, IndexModel, bson::Document};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use utoipa::ToSchema;

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Default, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    #[default]
    User,
}

#[derive(Clone, Serialize, Deserialize, Debug, ToSchema)]
pub struct User {
    username: String,
    password: String,
    role: Role,
    #[serde(default)]
    read_access: HashSet<String>,
    #[serde(default)]
    write_access: HashSet<String>,
}

#[derive(Clone)]
pub struct UsersDB {
    name: String,
    secret: String,
    client: Client,
}

impl UsersDB {
    pub async fn new(name: String, secret: String, connection_string: String) -> Result<Self, DatabaseError> {
        let client = Client::with_uri_str(connection_string).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let db = client.database(&name);
        let collection_names = db.list_collection_names().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        if collection_names.is_empty() {
            let users_collection = db.collection::<Document>("users");
            let index = IndexModel::builder().keys(doc! { "username": 1 }).options(IndexOptions::builder().unique(true).build()).build();
            users_collection.create_index(index).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
            let initial_username = std::env::var("INITIAL_ADMIN_USERNAME").unwrap_or_else(|_| "admin".to_owned());
            let initial_password = std::env::var("INITIAL_ADMIN_PASSWORD").unwrap_or_else(|_| "admin".to_owned());
            users_collection.insert_one(doc! { "username": initial_username, "password": hash_password(initial_password.as_str()), "role": "admin" }).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        }
        Ok(UsersDB { name, secret, client })
    }

    pub async fn default() -> Result<Self, DatabaseError> {
        let name = std::env::var("USERS_DB_NAME").unwrap_or_else(|_| "coco_users".to_string());
        let host = std::env::var("USERS_DB_HOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("USERS_DB_PORT").unwrap_or_else(|_| "27017".to_string()).parse().unwrap_or(27017);
        let connection_string = format!("mongodb://{}:{}/{}", host, port, name);
        let secret = std::env::var("JWT_SECRET").unwrap_or_else(|_| "default_secret".to_owned());
        Self::new(name, secret, connection_string).await
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn get_users(&self) -> Result<Vec<User>, DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<User>("users");
        let cursor = collection.find(doc! {}).await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        let users: Vec<User> = cursor.try_collect().await.map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;
        Ok(users)
    }

    pub async fn get_user(&self, username: &str, password: &str) -> Result<User, DatabaseError> {
        let db = self.client.database(&self.name);
        let users_collection = db.collection::<User>("users");
        let filter = doc! { "username": username };
        let user = users_collection.find_one(filter).await.map_err(|e| DatabaseError::NotFound(e.to_string()))?;
        if let Some(user) = user { if verify_password(password, &user.password) { Ok(user) } else { Err(DatabaseError::NotFound("Invalid username or password".to_string())) } } else { Err(DatabaseError::NotFound("Invalid username or password".to_string())) }
    }

    pub async fn get_user_by_username(&self, username: &str) -> Result<User, DatabaseError> {
        let db = self.client.database(&self.name);
        let users_collection = db.collection::<User>("users");
        let filter = doc! { "username": username };
        let user = users_collection.find_one(filter).await.map_err(|e| DatabaseError::NotFound(e.to_string()))?;
        user.ok_or_else(|| DatabaseError::NotFound("User not found".to_string()))
    }

    pub async fn create_user(&self, username: &str, password: &str, role: Role) -> Result<(), DatabaseError> {
        let db = self.client.database(&self.name);
        let collection = db.collection::<User>("users");
        let new_user = User {
            username: username.to_owned(),
            password: hash_password(password),
            role,
            read_access: HashSet::new(),
            write_access: HashSet::new(),
        };
        collection.insert_one(new_user).await.map_err(|e| if e.to_string().contains("duplicate key error") { DatabaseError::Exists(username.to_owned()) } else { DatabaseError::ConnectionError(e.to_string()) })?;
        Ok(())
    }
}

fn hash_password(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default().hash_password(password.as_bytes(), &salt).unwrap().to_string()
}

fn verify_password(password: &str, hash: &str) -> bool {
    let parsed_hash = PasswordHash::new(hash).unwrap();
    Argon2::default().verify_password(password.as_bytes(), &parsed_hash).is_ok()
}
