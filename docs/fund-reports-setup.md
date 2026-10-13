# Личный импорт отчётов LAIF: настройка

Поток: Gmail → Resend → DataHunt → OpenRouter → база → Google Sheets.
Две формулы возвращают сумму и дату оценки последнего проверенного отчёта.
До заполнения серверных секретов импорт выключен. Нового экрана в DataHunt нет.

## 1. Адрес приёма Resend

Откройте [Resend](https://resend.com/) → **Emails → Receiving**.
Найдите **Receiving address** вида `<anything>@<your-id>.resend.app`.
Используйте `laif` вместо `<anything>` и свой настоящий идентификатор домена,
например `laif@your-id.resend.app`. DNS для этого адреса менять не нужно.
Подключённый вами домен можно использовать для отправки уведомлений:
например `datahunt@your-domain.com`.

Если хочется принимать письма на своём домене, включите Receiving для отдельного
поддомена `reports.your-domain.com` и добавьте MX, указанный Resend.
Не заменяйте MX основного домена, если там уже работает ваша почта.
[Приём писем](https://resend.com/docs/dashboard/receiving/introduction).

## 2. Пересылка Gmail

1. Gmail на компьютере → настройки → **Все настройки → Пересылка и POP/IMAP**.
2. **Добавить адрес пересылки** → вставьте ваш адрес Resend.
3. В Resend → **Emails → Receiving** откройте письмо подтверждения Gmail
   и перейдите по ссылке подтверждения.
4. В Gmail создайте фильтр: точный отправитель отчётов, тема LAIF, наличие вложения.
   Сначала выполните поиск и убедитесь, что он находит нужные письма.
5. В фильтре включите **Переслать на** ваш адрес Resend. Глобальная пересылка всей
   почты не нужна. Исходные письма оставьте в Gmail.

Письмо подтверждения не обрабатывается как финансовый отчёт. Фильтр работает для
новых писем. Для старого отчёта нужна новая доставка PDF. При ручной пересылке
отправителем станет ваш Gmail: для такого теста согласуйте с оператором временного
разрешённого отправителя, затем верните отправителя фонда. Сервер принимает один
настроенный email отправителя.
[Инструкция Gmail](https://support.google.com/mail/answer/10957).

## 3. ID аккаунта DataHunt

ID — внутренний идентификатор DataHunt, а не email. Войдите в
[DataHunt](https://crypto.lisacorp.com/), откройте инструменты разработчика браузера
→ **Console** и выполните код ниже. Он создаёт или переиспользует существующий
токен для Sheets, но выводит только ID аккаунта:

```javascript
(async () => {
  const response = await fetch('https://hunt.data.lisacorp.com/web3/sheets-token', {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + localStorage.getItem('data_hunt_token') },
  });
  if (!response.ok) throw new Error('Войдите в DataHunt и повторите: ' + response.status);
  const result = await response.json();
  console.log('ID аккаунта DataHunt:', result.account_id);
})();
```

ID можно передать для настройки. Токены, API-ключи и ссылки с токенами держите у себя;
они не должны попадать в Git или сообщения.

## 4. Ключи и webhook

**Resend:** **API Keys → Create API Key** → отдельный ключ для DataHunt с **Full access**.
Он нужен для чтения входящих вложений и отправки уведомлений. Ключ с правом только
отправки не подходит. Проверьте статус **Verified** у вашего домена отправки.

В **Webhooks** создайте endpoint:

```text
https://hunt.data.lisacorp.com/webhooks/resend
```

Выберите **email.received**. На странице webhook скопируйте **Signing secret**,
начинающийся с `whsec_`, в серверный секрет. До развёртывания нового backend
этот endpoint ещё не работает.
[Проверка webhook](https://resend.com/docs/webhooks/verify-webhooks-requests).

**OpenRouter:** [Keys](https://openrouter.ai/settings/keys) → отдельный ключ с лимитом
**$1 и месячным сбросом**. Пополнение баланса оплачивается отдельно. Отключите
логирование входов/выходов и включите ZDR на уровне аккаунта: это также исключает
кеширование ответов сервисом. Сервер дополнительно задаёт `provider.zdr=true`,
`data_collection=deny`, `require_parameters=true`. Если подходящий провайдер
недоступен, импорт завершится ошибкой; конфиденциальность не будет ослаблена.
[Приватность](https://openrouter.ai/blog/insights/zero-data-retention/).

Модель по умолчанию — `google/gemini-3.1-flash-lite`. В модель уходят только
две строки с подписями, суммой и датой, без имени и почтового адреса.

## 5. Настройка сервера

Эту часть выполняет оператор DataHunt. Вам достаточно подготовить значения из
предыдущих шагов; Kubernetes обычному пользователю не нужен.

| Переменная | Значение |
| --- | --- |
| `FUND_REPORTS_ENABLED` | `true` после заполнения остальных параметров |
| `FUND_REPORT_ACCOUNT_ID` | ID из шага 3 |
| `FUND_REPORT_RECIPIENT` | Точный адрес приёма Resend |
| `FUND_REPORT_ALLOWED_SENDER` | Email отправителя оригинальных отчётов из Gmail |
| `FUND_REPORT_PDF_PASSWORD` | Пароль PDF, только в секрете |
| `FUND_REPORT_RESEND_API_KEY` | Отдельный ключ Resend с Full access |
| `FUND_REPORT_WEBHOOK_SECRET` | Signing secret webhook |
| `FUND_REPORT_NOTIFICATION_FROM` | Адрес на подтверждённом домене Resend |
| `FUND_REPORT_NOTIFICATION_TO` | Ваша обычная Gmail-почта |
| `FUND_REPORT_OPENROUTER_API_KEY` | Отдельный ключ OpenRouter с лимитом |
| `FUND_REPORT_MODEL` | Необязательно; `google/gemini-3.1-flash-lite` по умолчанию |

Для уведомлений подходит любой ваш обычный email. Специального аккаунта в Resend
для получения уведомлений не нужно.

Манифест backend читает настройки из Kubernetes Secret `data-hunt-fund-reports`
через `envFrom`. Ссылка необязательная: отсутствие секрета оставляет импорт выключенным
и не мешает остальным функциям сервера. Значения берутся из локального игнорируемого
файла `.secrets/fund-reports.env`; в Kubernetes нужно передавать только переменные
из таблицы выше, а не вспомогательные идентификаторы настройки.
Не помещайте открытые значения секретов в манифест.
Релиз требует отдельно одобренного развёртывания backend с новым digest.
Владельца существующего источника нельзя изменить переменной: сервер отклонит
такую конфигурацию, чтобы не передать чужие данные.

## 6. Две готовые формулы Sheets

После развёртывания и настройки откройте Console на странице DataHunt,
войдя в аккаунт владельца, и выполните:

```javascript
(async () => {
  const api = 'https://hunt.data.lisacorp.com';
  const headers = {
    Authorization: 'Bearer ' + localStorage.getItem('data_hunt_token'),
    'Content-Type': 'application/json',
  };
  const auth = await fetch(api + '/web3/sheets-token', { method: 'POST', headers });
  if (!auth.ok) throw new Error('Ошибка авторизации: ' + auth.status);
  const { access_token } = await auth.json();
  for (const field of ['amount', 'as_of']) {
    const response = await fetch(api + '/value-resources', {
      method: 'POST', headers,
      body: JSON.stringify({ source: 'fund-report', parameters: { field } }),
    });
    if (!response.ok) throw new Error('Импорт ещё не подключён: ' + response.status);
    const { id } = await response.json();
    const url = api + '/v/' + id + '?auth_token=' + encodeURIComponent(access_token);
    console.log(field === 'amount' ? 'Сумма:' : 'Дата оценки:', '=IMPORTDATA("' + url + '")');
  }
})();
```

Скопируйте формулы в две ячейки Sheets. До первого проверенного отчёта ссылки
возвращают 404, затем — число без разделителя тысяч и дату ISO. Отображение зависит
от локали Sheets; формат даты можно выбрать в меню таблицы. Google обычно проверяет
обновления `IMPORTDATA` раз в час при открытой таблице.
[Правила обновления](https://support.google.com/docs/answer/12188454?hl=en-GB).

## 7. Статус и ошибки

В той же Console:

```javascript
(async () => {
  const response = await fetch('https://hunt.data.lisacorp.com/fund-reports/status', {
    headers: { Authorization: 'Bearer ' + localStorage.getItem('data_hunt_token') },
  });
  if (!response.ok) throw new Error('Ошибка доступа: ' + response.status);
  console.log(await response.json());
})();
```

`as_of` — дата последнего проверенного отчёта, `latest_job.status` — состояние задачи.
`succeeded` означает успех, `duplicate` — повтор файла, `obsolete` — старый отчёт,
`retry` — временная ошибка, `failed` — окончательная ошибка, `conflict` — разные суммы
за одну дату. Ошибка не удаляет предыдущие значения.

При `failed` или `conflict` отправляется уведомление без финансовых данных.
`notification_status=sent` означает принятие письма Resend, а не подтверждение
доставки Gmail; `failed` — ошибка отправки, `unknown` — истечение безопасного окна
повтора. Resend хранит ключи идемпотентности 24 часа; сервер прекращает повторы
через 23 часа, чтобы не создать дубликат после истечения защиты.
[Идемпотентность](https://resend.com/docs/dashboard/emails/idempotency-keys).

## Ограничения первой версии

- Один личный фонд LAIF, один владелец, один разрешённый отправитель.
- PDF с текстовым слоем, максимум 10 MiB и 10 страниц. OCR для сканов не включён.
- Неизвестные форматы не угадываются: остаются последние проверенные значения.
- После смены пароля/формата оператор исправляет настройку и заново ставит
  неуспешную задачу в очередь. Повторная доставка того же файла сама по себе
  не запускает обработку повторно.
- После конфликта нужна ручная проверка; автоматической перезаписи за ту же дату нет.
- PDF и исходный текст не сохраняются в DataHunt. Gmail и Resend хранят письма
  по своим правилам. Истории баланса нет: только последний результат и сведения задач.
