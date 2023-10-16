from unittest.mock import Mock
import asynctest
from src.common_functions import safe_json


class TestSafeJson(asynctest.TestCase):

    async def test_safe_json_valid_json(self):
        # Создаем мок-объект response с методом json, который возвращает валидный JSON
        response = Mock()
        response.json = Mock(return_value={'key': 'value'})

        # Вызываем функцию safe_json
        result = await safe_json(response)

        # Проверяем, что результат равен валидному JSON
        self.assertEqual(result, {'key': 'value'})

    async def test_safe_json_invalid_json(self):
        # Создаем мок-объект response с методом json, который вызывает исключение
        response = Mock()
        response.json = Mock(side_effect=Exception('Invalid JSON'))

        # Вызываем функцию safe_json
        result = await safe_json(response)

        # Проверяем, что результат равен пустому словарю
        self.assertEqual(result, {})


if __name__ == '__main__':
    asynctest.main()
