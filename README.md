# PrimeScope

## Пайплайн (структура кроків)
1. **validate** — базова перевірка вмісту. Директорії беруться з
   `configs/schemas.yml` (`validate.datasets.*.dir`).
2. **collect** — відкриття сирих файлів з каталогу `raw`.
3. **normalize** — приведення даних до уніфікованого вигляду.
4. **interim** — формування проміжних артефактів.
5. **checks** — додаткові "ручні" перевірки.
6. **report** — підготовка фінальних звітів.

Кроки `validate` і `collect` використовують утиліти роботи з CSV з
`src/app/collectors/files.py`.

## Запуск
```bash
python scripts/processor.py           # help
python scripts/processor.py run       # повний цикл з новими кроками
```

## CLI: опція run

### Призначення
Запуск повного циклу: `validate → collect → normalize → interim → checks → report`.

### Використання
```bash
python3 scripts/processor.py run [опції]
python scripts/processor.py run [опції]
```

### Прапорці
- `--from STEP` — почати з кроку (допустимо: `validate|collect|normalize|interim|checks|report`)
- `--to STEP` — закінчити на кроці
- `--skip STEP[,STEP]` — пропустити вказані кроки
- `--dry-run` — лише показати план без запису файлів
- `--clean-first` — очистити `data/interim` перед запуском (окрім `*.example.csv`)
- `--yes` — автоматично підтверджувати потенційно руйнівні дії (для `--clean-first`)

### Приклади
- Повний цикл:
  ```bash
  python3 scripts/processor.py run
  ```
- Сухий прогін (без змін на диску):
  ```bash
  python3 scripts/processor.py run --dry-run
  ```
- Очистити проміжні файли й запустити цикл:
  ```bash
  python3 scripts/processor.py run --clean-first --yes
  ```
- Запустити частину пайплайну (лише від `normalize` до `report`):
  ```bash
  python3 scripts/processor.py run --from normalize --to report
  ```
- Пропустити додаткові перевірки:
  ```bash
  python3 scripts/processor.py run --skip checks
  ```
- Запустити тільки один крок (лише `collect`):
  ```bash
  python3 scripts/processor.py run --from collect --to collect
  ```
- Пропустити кілька кроків:
  ```bash
  python3 scripts/processor.py run --skip normalize,checks
  ```

### Допустимі кроки
`validate`, `collect`, `normalize`, `interim`, `checks`, `report`.

## validate
На початку кроку `validate` перевіряється наявність хоча б одного CSV-файла
(окрім `*example.csv`) у директоріях `data/raw/siem`, `data/raw/dhcp`,
`data/raw/ubiq`. Якщо в усіх трьох директоріях немає жодного такого файлу —
крок завершується з помилкою, і наступні етапи не запускаються.

Приклади логів:

```text
# Відсутні файли у всіх директоріях
▶ step=validate status=start
validate: no csv in: data/raw/siem
validate: no csv in: data/raw/dhcp
validate: no csv in: data/raw/ubiq
validate: no required csv found across (siem, dhcp, ubiq): need at least one *.csv (excluding *example.csv)
validate: errors summary: required_any_missing=1
✖ step=validate status=fail duration=0.01s

# Файли є хоча б в одній директорії
▶ step=validate status=start
validate: files in data/raw/siem: logs.csv
validate: no csv in: data/raw/dhcp
validate: no csv in: data/raw/ubiq
validate: errors summary: files_with_confusables=0, files_with_content_errors=0, total_issues=0
✓ step=validate status=done duration=0.01s
```

## validate.rules
У `configs/schemas.yml` перелічені правила для кроку `validate`. Кожне правило
може мати параметр `allow_literals` — список значень, які приймаються без
перевірки формату. Наприклад, правило `ip_or_literals` дозволяє рядок
`"N/A"` у полі з IP-адресою:

```yaml
validate:
  rules:
    ip_or_literals:
      kind: ip
      version: any
      allow_literals: ["N/A"]
  datasets:
    arm:
      fields:
        ip:
          headers: ["IP","ip_address","ip-address"]
          check: ip_or_literals
          required: true
    mkp:
      fields:
        ip:
          headers: ["IP","ip_address","ip-address"]
          check: ip_or_literals
          required: false
```

Аналогічний підхід використовується для MAC через правило `mac_or_literals`.

## Маніфест валідації
Після успішного кроку `validate` створюється маніфест у теці `.pscope/`.
Основні файли:

- `.pscope/validate/<run_id>.json` — детальний опис перевірених файлів для
  кожного датасету.
- `.pscope/latest.json` — посилання на останній валідний маніфест.

Крок `collect` за замовчуванням читає `.pscope/latest.json` і використовує
перелік файлів з маніфесту без повторної перевірки заголовків. Якщо маніфест
відсутній або застарів (змінилась `configs/schemas.yml` чи самі CSV), `collect`
завершується з помилкою і просить повторно запустити `validate`.

Тека `.pscope/` додана у `.gitignore`, оскільки містить тимчасові артефакти,
специфічні для локального запуску.

## Логування
За замовчуванням повідомлення рівня INFO виводяться у консоль та у файл `logs/pscope.log`.

Приклад виводу під час `run`:
```
▶ step=validate status=start
✓ step=validate status=done duration=0.102s
▶ step=collect status=start
✓ step=collect status=done duration=0.241s
...
run: done
```

