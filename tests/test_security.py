import contextlib
import datetime
import io
import ipaddress
import json
import tempfile
import unittest
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from agent_queue.cli import main
from agent_queue.config import read_raw, write_raw
from agent_queue.daemon import Daemon

from helpers import FakeRunner, add_route, home, install, wait_for

PRODUCER_HASH = (
    "$argon2id$v=19$m=19456,t=2,p=1$J26PPIiAh5Rjj7/Zt5vH5w$"
    "nqsWb3NdC2u4k8k+eFUbF31vKRL+0z0yfMl5mTTUaBc"
)


def make_tls(root: Path) -> Path:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Test CA")])
    now = datetime.datetime.now(datetime.timezone.utc)
    ca = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    server = (
        x509.CertificateBuilder()
        .subject_name(server_name)
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    (root / "ca.pem").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    (root / "server.pem").write_bytes(server.public_bytes(serialization.Encoding.PEM))
    (root / "server.key").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    (root / "password").write_text("producer-secret")
    (root / "security.json").write_text(
        json.dumps(
            {
                "tls": {"cert": "server.pem", "key": "server.key"},
                "users": {
                    "producer": {
                        "password_hash": PRODUCER_HASH,
                        "permissions": {"publish": ["ticket.create"]},
                    }
                },
            }
        )
    )
    return root / "ca.pem"


class SecureIngressTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.paths = home(self.root)
        self.source = install(self.paths, self.root)
        add_route(self.paths, "ticket.create", self.source)
        raw = read_raw(self.paths.config)
        raw["amqp"]["port"] = 0
        raw["amqp"]["security"] = "security.json"
        write_raw(self.paths.config, raw)
        self.ca = make_tls(self.paths.home)
        self.runner = FakeRunner()
        self.daemon = Daemon(self.paths, runner=self.runner)
        self.daemon.start()
        wait_for(lambda: self.daemon.status()["routes"][0]["state"] == "ready", 5)

    def tearDown(self):
        self.daemon.stop()
        self.directory.cleanup()

    def cli(self, *arguments):
        output = io.StringIO()
        errors = io.StringIO()
        with contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code = main(["--home", str(self.paths.home), *arguments])
        return code, output.getvalue(), errors.getvalue()

    def send_args(self, *extra):
        return (
            "send",
            "--topic",
            "ticket.create",
            "--message",
            '{"order": 1}',
            "--tls-ca",
            str(self.ca),
            "--tls-server-name",
            "127.0.0.1",
            "--user",
            "producer",
            "--password-file",
            str(self.paths.home / "password"),
            *extra,
        )

    def take_result(self):
        def attempt():
            deliveries = self.daemon.ante.take(
                "ticket.create.results", max=1, wait_ms=50
            )
            if not deliveries:
                return None
            payload = deliveries[0].payload
            deliveries[0].ack()
            return payload

        return wait_for(attempt, 5)

    def test_tls_send_with_credentials(self):
        code, output, errors = self.cli(*self.send_args())
        self.assertEqual(code, 0, errors)
        self.assertIn("over TLS", output)
        result = self.take_result()
        self.assertEqual(result["status"], "succeeded")
        status = self.daemon.status()
        self.assertTrue(status["daemon"]["amqp"]["tls"])

    def test_plaintext_and_wrong_password_fail(self):
        code, _, errors = self.cli("send", "--topic", "ticket.create", "--message", "x")
        self.assertEqual(code, 64)
        self.assertIn("--tls-ca", errors)

        password = self.paths.home / "password"
        password.write_text("wrong")
        code, _, errors = self.cli(*self.send_args())
        self.assertEqual(code, 69)
        self.assertIn("AMQP publish failed", errors)


if __name__ == "__main__":
    unittest.main()
