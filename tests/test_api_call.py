import aiohttp
import asynctest
from unittest.mock import Mock
from src.common_functions import api_call


class TestApiCall(asynctest.TestCase):
    """
    В этом тесте созданы два тестовых случая:
    test_api_call_success: Проверяет, что функция api_call работает правильно при успешном вызове API.
    Мы используем unittest.mock.patch для замены aiohttp.ClientSession().get() на макетный объект,
    который возвращает ожидаемый объект response.
    test_api_call_failure: Проверяет, что функция api_call корректно обрабатывает ошибку при вызове API.
    Мы снова используем unittest.mock.patch, чтобы заменить aiohttp.ClientSession().get() на макетный объект,
    который вызывает aiohttp.ClientError.
    Оба теста проверяют ожидаемые результаты и взаимодействие с aiohttp.ClientSession().get().
    """
    async def test_api_call_success(self):
        url = "https://api.com/some_endpoint"  # изменить на актуальный URL
        response = Mock()
        response.raise_for_status = Mock()  # Mocking raise_for_status method

        # Mocking aiohttp.ClientSession().get() to return response
        session_mock = Mock()
        session_mock.get = Mock(return_value=response)

        with asynctest.patch('aiohttp.ClientSession', return_value=session_mock):
            result = await api_call(url)

        self.assertEqual(result, response)
        session_mock.get.assert_called_with(url, headers={'...': 'api_key', '...': 'api.com'})

    async def test_api_call_failure(self):
        url = "https://api.com/some_endpoint"  # изменить на актуальный URL

        # Mocking aiohttp.ClientSession().get() to raise an exception
        session_mock = Mock()
        session_mock.get = Mock(side_effect=aiohttp.ClientError)

        with asynctest.patch('aiohttp.ClientSession', return_value=session_mock):
            result = await api_call(url)

        self.assertIsNone(result)


if __name__ == '__main__':
    asynctest.main()
