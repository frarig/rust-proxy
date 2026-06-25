// Импортируем trait Context из anyhow.
//
// Он позволяет добавлять нормальный текст к ошибкам:
// .context("failed to build config")?
//
// Без него ошибка была бы менее понятной.
use anyhow::Context;

// Config — основной builder конфигурации.
// Environment — источник настроек из переменных окружения.
// File — источник настроек из файла.
use config::{Config, Environment, File};

// Импортируем Deserialize из serde.
//
// Он нужен, чтобы config мог автоматически превратить TOML/env
// в Rust-структуры.
use serde::Deserialize;

// Основная структура всех настроек приложения.
//
// Debug — можно печатать через {:?}
// Clone — можно копировать структуру через .clone()
// Deserialize — можно десериализовать из конфига
#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    // Секция [server] из config/default.toml.
    pub server: ServerSettings,

    // Секция [upstream] из config/default.toml.
    pub upstream: UpstreamSettings,
}

// Настройки самого proxy-сервера.
//
// Debug — можно печатать через {:?}
// Clone — можно копировать структуру через .clone()
// Deserialize — можно десериализовать из конфига
#[derive(Debug, Clone, Deserialize)]
pub struct ServerSettings {
    // Адрес, на котором будет слушать наш прокси.
    pub listen_addr: String,

    // Максимальное количество одновременных соединений.
    pub max_connections: usize,
}

// Настройки backend/upstream-сервера.
//
// Debug — можно печатать через {:?}
// Clone — можно копировать структуру через .clone()
// Deserialize — можно десериализовать из конфига
#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamSettings {
    // Адрес сервера, куда прокси будет перенаправлять трафик.
    //
    // В docker-compose это:
    // "echo-server:9000"
    pub addr: String,

    // Таймаут подключения к backend в миллисекундах.
    //
    // Если backend не отвечает — соединение не будет висеть вечно.
    pub connect_timeout_ms: u64,
}

// Добавляем методы к структуре Settings.
impl Settings {
    // Публичный ассоциированный метод.
    // Self здесь означает Settings.
    pub fn load() -> anyhow::Result<Self> {

        Config::builder()
            // Добавляем источник настроек из файла config/default.toml. required(true) если файла нет — это ошибка запуска.
            .add_source(File::with_name("config/default").required(true))
            // Добавляем источник настроек из переменных окружения.
            // separator("__") означает вложенные поля через двойное подчёркивание (PROXY__SERVER__LISTEN_ADDR => server.listen_addr).
            // try_parsing(true) пытается преобразовать строки в числа/bool.
            .add_source(Environment::with_prefix("PROXY").separator("__").try_parsing(true))
            // Собираем Config.
            .build()
            // Если build упал, добавляем понятный контекст ошибки.
            .context("Failed to build application config")?
            // Преобразуем Config в Settings.
            // Поля в TOML должны совпадать со структурами:
            // server.listen_addr
            // server.max_connections
            // upstream.addr
            // upstream.connect_timeout_ms
            .try_deserialize()
            // Если десериализация упала, добавляем понятный контекст.
            .context("Failed to deserialize application config")
    }
}
