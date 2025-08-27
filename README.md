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

## validate.rules
У `configs/schemas.yml` перелічені правила для кроку `validate`. Кожне правило
може мати параметр `allow_literals` — список значень, які приймаються без
перевірки формату. Наприклад, правило `mac_or_dash` дозволяє рядок `'-'` у
полі з MAC-адресою:

```yaml
validate:
  rules:
    mac_or_dash:
      kind: mac
      allow_literals: ["-"]
  datasets:
    arm:
      fields:
        randmac:
          headers: ["Random MAC","randmac","dynamic_mac"]
          check: mac_or_dash
          required: false
    mkp:
      fields:
        randmac:
          headers: ["Динамічний MAC","Dynamic MAC","randmac"]
          check: mac_or_dash
          required: false
```

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

