# PrimeScope

## CLI запуск (script/processor.py)
Єдиний вхід для керування опціями PrimeScope.
Опції зберігаються у `src/app/options/options.py` через функцію `get_options()`.
За замовчуванням запуск без аргументів викликає опцію `help`.
На цьому етапі доступна лише опція `help`.

### Приклади
```
# показати довідку
python script/processor.py

# те саме явно
python script/processor.py help

# довідка по конкретній опції (демо з help)
python script/processor.py help help
```

### Коди завершення
- 0 - успіх
- 2 - невідома опція/некоректне використання
- 1 - фатальна помилка

### Розширення
Щоб додати нову опцію, додайте запис у `get_options()` з полями `about`, `usage` та `handler`.
Опції не прописуються у `script/processor.py`, тільки в `options.py`.
