import unittest
from unittest.mock import patch, mock_open, MagicMock
from utils.Connector import *

class TestConnector(unittest.TestCase):
    @patch("builtins.open", new_callable=mock_open, read_data='{"test_token": {"access_token": "mock_token"}}')
    @patch.object(Connector, 'load_token_info')
    def test_load_token_info(self, mock_load_token_info, mock_file):
        mock_logger = MagicMock()
        
        #Correct key
        mock_load_token_info.return_value = {"access_token": "mock_token"}
        connector = Connector(mock_logger, "test_token")
        token_info = connector.load_token_info()
        self.assertEqual(token_info["access_token"],"mock_token")
        mock_logger.info.assert_called_with("Load Token Info: Loaded test_token Token Data.")

        #Test non-existent key
        mock_load_token_info.return_value = {}
        connector = Connector(mock_logger, "testt_token")
        token_info = connector.load_token_info()
        self.assertEqual(token_info, {})
        mock_logger.warning.assert_called_with("Load Token Info: testt_token not found in token storage.")

if __name__ == "__main__":
    unittest.main()