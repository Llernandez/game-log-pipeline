import unittest

from game_log_pipeline.services import snowflake_auth


class SnowflakeAuthTests(unittest.TestCase):
    def test_password_when_no_key(self):
        self.assertEqual(snowflake_auth({"GLP_SNOWFLAKE_PASSWORD": "pw"}), {"password": "pw"})

    def test_key_pair_preferred_over_password(self):
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
        except ImportError:
            self.skipTest("cryptography ships with the services extra")
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption()).decode()
        auth = snowflake_auth({"GLP_SNOWFLAKE_PRIVATE_KEY": pem, "GLP_SNOWFLAKE_PASSWORD": "pw"})
        self.assertEqual(set(auth), {"private_key"})
        self.assertIsInstance(auth["private_key"], bytes)


if __name__ == "__main__":
    unittest.main()
