mod settings;
mod proxy;

use std::fmt::Debug;
// Импортируем derive-макрос Parser из crate clap.
// Он позволяет автоматически разобрать CLI-аргументы в структуру Cli.
use clap::Parser;
use settings::Settings;
// Импортируем EnvFilter для настройки уровня логирования.
// Например: info, debug, rust_proxy=debug.
use tracing_subscriber::EnvFilter;

// Описываем структуру CLI-аргументов.
//
// #[derive(Debug, Parser)] автоматически генерирует:
// - Debug: возможность печатать структуру через {:?}
// - Parser: возможность вызвать Cli::parse()
#[derive(Debug, Parser)]
// Метаданные CLI-приложения.
// Используются clap при генерации help-сообщения.
#[command(name = "rust-proxy")]
#[command(about = "High-performance TCP proxy written in Rust")]
struct Cli {
    // Описываем CLI-аргумент:
    //
    // --log-level
    //
    // env = "RUST_PROXY_LOG" означает:
    // значение можно передать через переменную окружения.
    //
    // default_value = "info" означает:
    // если ничего не передали, используем уровень info.
    #[arg(long, env = "RUST_PROXY_LOG", default_value = "info")]
    log_level: String,
}

// Точка входа в приложение.
//
// #[tokio::main] превращает async main в обычный main,
// создаёт Tokio runtime и запускает внутри него async-код.
//
// Без этого async fn main() сам по себе не запустится.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Настраиваем логирование через tracing-subscriber.
    tracing_subscriber::fmt()
        .json() // Логи будут в JSON.
        .with_env_filter(EnvFilter::new(cli.log_level)) // Применяем фильтр логирования из cli.log_level
        .init(); // Инициализируем глобальный subscriber. После этого tracing::info!, error!, warn! начнут писать логи.

    // Загружаем настройки приложения из config/default.toml
    // и переменных окружения PROXY__...
    //
    // Если конфиг битый — приложение завершится с ошибкой.
    let settings = Settings::load()?;

    // Запускаем основной proxy-сервер.
    //
    // .await нужен, потому что run — async функция.
    //
    // Если proxy::run вернёт ошибку, она будет возвращена из main.
    proxy::run(settings).await
}
