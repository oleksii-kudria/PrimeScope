# PrimeScope

## CLI запуск (scripts/processor.py)
Єдиний вхід для керування опціями PrimeScope.
Опції зберігаються у `src/app/options/options.py` через функцію `get_options()`.
За замовчуванням запуск без аргументів викликає опцію `help`.
На цьому етапі доступна лише опція `help`.

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
