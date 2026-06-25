use crate::settings::Settings;
// Импортируем Context из anyhow для добавления текста к ошибкам.
use anyhow::Context;

// Arc — atomic reference counted pointer.
//
// Нужен, чтобы безопасно разделять Semaphore между async тасками.
use std::sync::Arc;

// Duration — тип для хранения длительности времени.
use std::time::Duration;

// tokio::io нужен для copy_bidirectional.
use tokio::io;

// TcpListener — серверный TCP socket.
// TcpStream — TCP-соединение.
use tokio::net::{TcpListener, TcpStream};

// signal нужен, чтобы слушать Ctrl+C.
use tokio::signal;

// Semaphore ограничивает количество одновременных соединений.
use tokio::sync::Semaphore;

// timeout ограничивает время выполнения async-операции.
use tokio::time::timeout;

// Макросы логирования.
use tracing::{error, info, warn};

// Главная функция proxy-сервера.
//
// Принимает Settings по значению.
// То есть main передаёт владение настройками сюда.
pub async fn run(settings: Settings) -> anyhow::Result<()> {
    // Помещаем Settings внутрь Arc (Atomic Reference Counted).
    //
    // Arc позволяет нескольким потокам или async-задачам безопасно
    // совместно владеть одним объектом.
    //
    // Сам объект Settings НЕ копируется.
    // Arc выделяет память в куче и начинает отслеживать количество владельцев.
    //
    // Сейчас владельцем является только эта переменная settings.
    let settings = Arc::new(settings);

    // Создаём TCP listener на адресе из конфига.
    let listener = TcpListener::bind(&settings.server.listen_addr)
        // .await — потому что bind async в Tokio.
        .await
        // Если bind упал, добавляем адрес в текст ошибки.
        .with_context(|| format!("Failed to bind to {}", settings.server.listen_addr))?;

    // Создаём лимитер соединений.
    //
    // Semaphore::new(1024) означает:
    // одновременно можно держать максимум 1024 permit'а.
    //
    // Arc нужен, потому что limiter будет использоваться из разных tokio::spawn тасок.
    let connection_limiter = Arc::new(Semaphore::new(settings.server.max_connections));

    // Пишем стартовый лог.
    info!(
        listen_addr = %settings.server.listen_addr,
        upstream_addr = %settings.upstream.addr,
        max_connections = settings.server.max_connections,
        "Starting proxy server"
    );

    // Сервер постоянно принимает новые TCP-соединения (бесконечный цикл accept).
    loop {
        // tokio::select! ждёт несколько async-событий одновременно.
        //
        // Здесь мы одновременно ждём:
        // 1. новое TCP-соединение
        // 2. Ctrl+C
        tokio::select! {
            // Ветка accept. Ждёт новое входящее соединение.
            accept_result = listener.accept() => {
                // Разбираем результат accept.
                let (client, client_addr) = match accept_result {
                    // Если успешно — получаем => client: TcpStream и client_addr: SocketAddr
                    Ok(val) => val,

                    // Если accept завершился ошибкой, логируем и продолжаем цикл.
                    Err(e) => {
                        error!(%e, "Failed to accept incoming connection");
                        continue;
                    }
                };

                // Пытаемся взять permit из Semaphore.
                //
                // try_acquire_owned() не ждёт. Если лимит исчерпан — сразу возвращает ошибку.
                let permit = match connection_limiter.clone().try_acquire_owned() {
                    // Permit получен — соединение можно обрабатывать.
                    Ok(val) => val,

                    // Permit не получен — слишком много соединений.
                    Err(_) => {
                        warn!(
                            client_addr = %client_addr,
                            "Connection rejected: max connections limit reached"
                        );
                        // client здесь будет автоматически закрыт, потому что переменная выйдет из scope.
                        continue;
                    }
                };

                // Создаём ещё одного владельца того же самого объекта Settings.
                //
                // Arc::clone() НЕ копирует сам Settings.
                // Увеличивается только счётчик ссылок внутри Arc.
                //
                // Это дешёвая операция O(1), поэтому её можно выполнять для каждого нового соединения.
                //
                // Полученный Arc передаётся в новую async-задачу.
                let settings = Arc::clone(&settings);

                // tokio::spawn создаёт новую независимую async-задачу.
                //
                // После запуска эта задача может жить намного дольше,
                // чем текущая функция run().
                //
                // Поэтому она не может просто взять ссылку (&Settings).
                // Компилятор не сможет гарантировать,
                // что объект Settings будет существовать всё время жизни задачи.
                //
                // Arc решает эту проблему:
                // задача получает собственного владельца Settings,
                // поэтому объект будет существовать,
                // пока существует хотя бы один Arc.
                tokio::spawn(async move {
                    // Кладём permit внутрь таски.
                    //
                    // Пока _permit живёт, слот Semaphore занят.
                    // Когда task завершится, _permit будет дропнут и слот автоматически вернётся в Semaphore.
                    let _permit = permit;

                    // Обрабатываем соединение.
                    if let Err(e) = handle_connection(client, settings).await {
                        error!(
                            client_addr = %client_addr,
                            %e,
                            "Connection failed"
                        );
                    }
                });
            }

            // Ветка shutdown.
            //
            // signal::ctrl_c() ждёт Ctrl+C.
            shutdown_result = signal::ctrl_c() => {
                // Разбираем результат ожидания Ctrl+C.
                match shutdown_result {
                    // Ctrl+C успешно получен.
                    Ok(()) => info!("Shutting down server"),

                    // Ошибка при подписке/ожидании сигнала.
                    Err(e) => error!(%e, "Failed to listen for shutdown signal"),
                }
                // Выходим из loop.
                // После этого сервер перестанет принимать новые соединения.
                break;
            }
        }
    }

    // Логируем, что новые соединения больше не принимаются.
    info!("Proxy server stopped accepting new connections");

    // Возвращаем успешный результат.
    Ok(())
}

// Обработка одного TCP-соединения.
//
// client — соединение от пользователя к прокси.
// settings — настройки приложения.
async fn handle_connection(mut client: TcpStream, settings: Arc<Settings>) -> anyhow::Result<()> {
    // Создаём Duration из миллисекунд из конфига.
    let connection_timeout = Duration::from_millis(settings.upstream.connect_timeout_ms);

    // Подключаемся к upstream/backend с таймаутом.
    let mut upstream = timeout(connection_timeout, TcpStream::connect(&settings.upstream.addr))
        // ждем завершения timeout(...)
        .await
        // Первая ошибка возможна от timeout.
        //
        // Если TcpStream::connect не успел завершиться за connect_timeout, сюда попадёт elapsed error.
        .with_context(|| {
            format!(
                "upstream connection timeout after {} ms",
                settings.upstream.connect_timeout_ms
            )
        })?
        // Вторая ошибка возможна уже от TcpStream::connect.
        //
        // Например: DNS не resolved, connection refused, network unreachable.
        .with_context(|| format!("failed to connect to upstream {}", settings.upstream.addr))?;

    // Копируем байты в обе стороны:
    //
    // client -> upstream
    // upstream -> client
    //
    // Метод завершится, когда соединение закроется или произойдёт ошибка.
    let bytes = io::copy_bidirectional(&mut client, &mut upstream)
        .await
        .context("Failed to proxy traffic")?;

    // Логируем статистику соединения.
    //
    // bytes.0 — сколько байт ушло от client к upstream.
    // bytes.1 — сколько байт ушло от upstream к client.
    info!(
        client_to_upstream_bytes = bytes.0,
        upstream_to_client_bytes = bytes.1,
        "Connection closed"
    );

    // Возвращаем успешный результат.
    Ok(())
}