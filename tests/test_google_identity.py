import unittest
from unittest.mock import patch

from google.auth.exceptions import TransportError

from google_identity import (
    GoogleIdentityNotConfiguredError,
    GoogleIdentityUnavailableError,
    GoogleIdentityVerificationError,
    verify_google_identity_token,
)


class GoogleIdentityTest(unittest.TestCase):
    @patch("google_identity.GOOGLE_CLIENT_ID", "web-client-id")
    @patch("google_identity.id_token.verify_oauth2_token")
    def test_verifies_for_configured_audience_and_returns_subject(self, verifier):
        verifier.return_value = {"sub": "stable-google-subject"}

        subject = verify_google_identity_token("credential")

        self.assertEqual(subject, "stable-google-subject")
        args = verifier.call_args.args
        self.assertEqual(args[0], "credential")
        self.assertEqual(args[2], "web-client-id")

    @patch("google_identity.GOOGLE_CLIENT_ID", "web-client-id")
    @patch(
        "google_identity.id_token.verify_oauth2_token",
        side_effect=ValueError("bad token"),
    )
    def test_rejects_failed_google_verification(self, _verifier):
        with self.assertRaises(GoogleIdentityVerificationError):
            verify_google_identity_token("credential")

    @patch("google_identity.GOOGLE_CLIENT_ID", "web-client-id")
    @patch(
        "google_identity.id_token.verify_oauth2_token",
        side_effect=TransportError("Google unavailable"),
    )
    def test_reports_google_transport_failure(self, _verifier):
        with self.assertRaises(GoogleIdentityUnavailableError):
            verify_google_identity_token("credential")

    @patch("google_identity.GOOGLE_CLIENT_ID", "web-client-id")
    @patch("google_identity.id_token.verify_oauth2_token", return_value={})
    def test_rejects_token_without_subject(self, _verifier):
        with self.assertRaises(GoogleIdentityVerificationError):
            verify_google_identity_token("credential")

    @patch("google_identity.GOOGLE_CLIENT_ID", "")
    def test_reports_missing_client_configuration(self):
        with self.assertRaises(GoogleIdentityNotConfiguredError):
            verify_google_identity_token("credential")


if __name__ == "__main__":
    unittest.main()
