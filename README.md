# PrimeScope

## CLI запуск (scripts/processor.py)
Єдиний вхід для керування опціями PrimeScope.
Опції зберігаються у `src/app/options/options.py` через функцію `get_options()`.
За замовчуванням запуск без аргументів викликає опцію `help`.
Опції `help` та `run` демонструють підхід до розширення CLI.

### Приклади
```
# показати довідку
python scripts/processor.py

# те саме явно
python scripts/processor.py help

# довідка по конкретній опції (демо з help)
python scripts/processor.py help help
```

### Коди завершення
- 0 - успіх
- 2 - невідома опція/некоректне використання
- 1 - фатальна помилка

### Розширення
Щоб додати нову опцію, додайте запис у `get_options()` з полями `about`, `usage` та `handler`.
Опції не прописуються у `scripts/processor.py`, тільки в `options.py`.

## CLI: опція run

Опція `run` запускає повний прод-цикл обробки даних. Кроки за замовчуванням:
`collect → normalize → dedupe → verify → report`.

Опис циклів зберігається у `src/app/pipeline/flows.py`, а логіка запуску
розташована у `src/app/pipeline/runner.py`. Реєстр опцій знаходиться у
`src/app/options/options.py`.

### Приклади
```
# показати довідку
python scripts/processor.py
python scripts/processor.py help

# запустити повний цикл
python scripts/processor.py run

# обмежити діапазон кроків
python scripts/processor.py run --from normalize --to report

# пропустити кроки
python scripts/processor.py run --skip normalize,dedupe

# сухий прогін
python scripts/processor.py run --dry-run

# очистити інтерім перед запуском
python scripts/processor.py run --clean-first --yes
```

### Коди завершення
- 0 - успіх
- 2 - некоректні аргументи або відсутнє підтвердження для руйнівних дій
- 1 - фатальна помилка
