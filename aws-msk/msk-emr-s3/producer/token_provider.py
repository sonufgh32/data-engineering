from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

class TokenProvider:

    def token(self):

        token, expiry = MSKAuthTokenProvider.generate_auth_token(
            "ap-south-1"
        )

        return token