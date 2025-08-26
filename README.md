# PrimeScope

## Пайплайн (структура кроків)
1. **collect** — відкриття сирих файлів з каталогу `raw`.
2. **validate** — базова перевірка вмісту.
3. **normalize** — приведення даних до уніфікованого вигляду.
4. **interim** — формування проміжних артефактів.
5. **checks** — додаткові "ручні" перевірки.
6. **report** — підготовка фінальних звітів.

## Запуск
```bash
python scripts/processor.py           # help
python scripts/processor.py run       # повний цикл з новими кроками
```

## Логування
За замовчуванням повідомлення рівня INFO виводяться у консоль та у файл `logs/pscope.log`.

Приклад виводу під час `run`:
```
▶ step=collect status=start
✓ step=collect status=done duration=0.241s
▶ step=validate status=start
✓ step=validate status=done duration=0.102s
...
run: done
```

