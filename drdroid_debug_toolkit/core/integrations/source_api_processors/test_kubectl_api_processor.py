import sys
import unittest
from unittest.mock import Mock, patch, MagicMock

# Mock Django modules before importing
django_mock = MagicMock()
settings_mock = MagicMock()
settings_mock.NATIVE_KUBERNETES_API_MODE = True

sys.modules['django'] = django_mock
sys.modules['django.conf'] = MagicMock()
sys.modules['django.conf'].settings = settings_mock

# Mock core modules
processor_mock = MagicMock()
processor_mock.Processor = object  # Use object as base class
sys.modules['core'] = MagicMock()
sys.modules['core.integrations'] = MagicMock()
sys.modules['core.integrations.processor'] = processor_mock
sys.modules['core.settings'] = MagicMock()
sys.modules['core.settings'].EXTERNAL_CALL_TIMEOUT = 30

from kubectl_api_processor import KubectlApiProcessor


class TestKubectlApiProcessor(unittest.TestCase):
    """Unit tests for KubectlApiProcessor command parsing and approval logic."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a processor instance with native mode
        self.processor = KubectlApiProcessor()

    def test_strip_kubectl_prefix_basic(self):
        """Test stripping leading kubectl from commands."""
        result = KubectlApiProcessor.strip_kubectl_prefix("kubectl get pods")
        self.assertEqual(result, "get pods")

    def test_strip_kubectl_prefix_api_resources(self):
        """Test that api-resources subcommand is preserved correctly."""
        result = KubectlApiProcessor.strip_kubectl_prefix("kubectl api-resources")
        self.assertEqual(result, "api-resources")

    def test_strip_kubectl_prefix_no_kubectl(self):
        """Test command without kubectl prefix."""
        result = KubectlApiProcessor.strip_kubectl_prefix("get pods")
        self.assertEqual(result, "get pods")

    def test_strip_kubectl_prefix_with_whitespace(self):
        """Test stripping kubectl with extra whitespace."""
        result = KubectlApiProcessor.strip_kubectl_prefix("  kubectl   get pods  ")
        self.assertEqual(result, "get pods")

    def test_check_dangerous_operators_semicolon(self):
        """Test detection of dangerous semicolon operator."""
        result = KubectlApiProcessor.check_dangerous_operators("get pods; rm -rf /")
        self.assertTrue(result)

    def test_check_dangerous_operators_ampersand(self):
        """Test detection of dangerous && operator."""
        result = KubectlApiProcessor.check_dangerous_operators("get pods && delete pod foo")
        self.assertTrue(result)

    def test_check_dangerous_operators_redirect(self):
        """Test detection of dangerous redirect operators."""
        self.assertTrue(KubectlApiProcessor.check_dangerous_operators("get pods > /tmp/pods.txt"))
        self.assertTrue(KubectlApiProcessor.check_dangerous_operators("get pods >> /tmp/pods.txt"))
        self.assertTrue(KubectlApiProcessor.check_dangerous_operators("cat < /tmp/input.txt"))

    def test_check_dangerous_operators_command_substitution(self):
        """Test detection of command substitution."""
        self.assertTrue(KubectlApiProcessor.check_dangerous_operators("get pods $(whoami)"))
        self.assertTrue(KubectlApiProcessor.check_dangerous_operators("get pods `whoami`"))

    def test_check_dangerous_operators_safe_command(self):
        """Test that safe commands are not flagged."""
        result = KubectlApiProcessor.check_dangerous_operators("get pods -n kube-system")
        self.assertFalse(result)

    def test_is_safe_pipe_command_head(self):
        """Test that head is recognized as safe."""
        result = KubectlApiProcessor.is_safe_pipe_command("head -n 5")
        self.assertTrue(result)

    def test_is_safe_pipe_command_grep(self):
        """Test that grep is recognized as safe."""
        result = KubectlApiProcessor.is_safe_pipe_command("grep Running")
        self.assertTrue(result)

    def test_is_safe_pipe_command_unsafe(self):
        """Test that unsafe commands are rejected."""
        result = KubectlApiProcessor.is_safe_pipe_command("bash -c 'rm -rf /'")
        self.assertFalse(result)

    def test_requires_approval_get_command(self):
        """Test that get command does not require approval."""
        result = KubectlApiProcessor.requires_approval("get pods -A")
        self.assertFalse(result)

    def test_requires_approval_describe_command(self):
        """Test that describe command does not require approval."""
        result = KubectlApiProcessor.requires_approval("describe pod foo")
        self.assertFalse(result)

    def test_requires_approval_api_resources(self):
        """Test that api-resources does not require approval."""
        result = KubectlApiProcessor.requires_approval("api-resources")
        self.assertFalse(result)

    def test_requires_approval_logs_command(self):
        """Test that logs command does not require approval."""
        result = KubectlApiProcessor.requires_approval("logs my-pod -n default")
        self.assertFalse(result)

    def test_requires_approval_apply_command(self):
        """Test that apply command requires approval."""
        result = KubectlApiProcessor.requires_approval("apply -f deployment.yaml")
        self.assertTrue(result)

    def test_requires_approval_delete_command(self):
        """Test that delete command requires approval."""
        result = KubectlApiProcessor.requires_approval("delete pod my-pod")
        self.assertTrue(result)

    def test_requires_approval_scale_command(self):
        """Test that scale command requires approval."""
        result = KubectlApiProcessor.requires_approval("scale deployment my-dep --replicas=3")
        self.assertTrue(result)

    def test_requires_approval_rollout_restart(self):
        """Test that rollout restart requires approval."""
        result = KubectlApiProcessor.requires_approval("rollout restart deployment/my-dep")
        self.assertTrue(result)

    def test_requires_approval_rollout_status(self):
        """Test that rollout status does not require approval."""
        result = KubectlApiProcessor.requires_approval("rollout status deployment/my-dep")
        self.assertFalse(result)

    def test_requires_approval_rollout_history(self):
        """Test that rollout history does not require approval."""
        result = KubectlApiProcessor.requires_approval("rollout history deployment/my-dep")
        self.assertFalse(result)

    def test_requires_approval_get_rollout(self):
        """Test that get rollout does not require approval."""
        result = KubectlApiProcessor.requires_approval("get rollout foo -n bar -o yaml")
        self.assertFalse(result)

    def test_requires_approval_get_rollouts_crd(self):
        """Test that get rollouts.argoproj.io does not require approval."""
        result = KubectlApiProcessor.requires_approval("get rollouts.argoproj.io my-rollout -n default")
        self.assertFalse(result)

    @patch('kubectl_api_processor.subprocess.Popen')
    def test_execute_command_api_resources_success(self, mock_popen):
        """Test that api-resources command executes without rewriting."""
        # Mock successful execution
        mock_process = Mock()
        mock_process.returncode = 0
        mock_process.communicate.return_value = ("NAME\npods", "")
        mock_popen.return_value = mock_process

        result = self.processor.execute_command("kubectl api-resources", require_approval_check=False)

        # Verify the command was called correctly
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args[0][0]
        self.assertEqual(call_args[0], "kubectl")
        self.assertEqual(call_args[1], "api-resources")
        self.assertIn("NAME\npods", result)

    @patch('kubectl_api_processor.subprocess.Popen')
    def test_execute_command_get_pods_with_pipe(self, mock_popen):
        """Test that get pods with pipe to head is allowed without approval."""
        # Mock kubectl execution
        mock_kubectl_process = Mock()
        mock_kubectl_process.returncode = 0
        mock_kubectl_process.communicate.return_value = ("pod1\npod2\npod3\npod4\npod5\npod6", "")

        # Mock head execution
        mock_head_process = Mock()
        mock_head_process.returncode = 0
        mock_head_process.communicate.return_value = ("pod1\npod2\npod3\npod4\npod5", "")

        mock_popen.side_effect = [mock_kubectl_process, mock_head_process]

        result = self.processor.execute_command("kubectl get pods -A | head -n 5", require_approval_check=True)

        # Verify approval was not required and command executed
        self.assertEqual(mock_popen.call_count, 2)
        self.assertIn("pod1", result)

    def test_execute_command_apply_requires_approval(self):
        """Test that apply command raises error requiring approval."""
        with self.assertRaises(ValueError) as context:
            self.processor.execute_command("kubectl apply -f deployment.yaml", require_approval_check=True)

        self.assertIn("requires approval", str(context.exception))

    def test_execute_command_get_rollout_no_approval(self):
        """Test that get rollout with pipe does not require approval."""
        with patch('kubectl_api_processor.subprocess.Popen') as mock_popen:
            # Mock kubectl execution
            mock_kubectl_process = Mock()
            mock_kubectl_process.returncode = 0
            mock_kubectl_process.communicate.return_value = ("rollout data here", "")

            # Mock head execution
            mock_head_process = Mock()
            mock_head_process.returncode = 0
            mock_head_process.communicate.return_value = ("rollout data here", "")

            mock_popen.side_effect = [mock_kubectl_process, mock_head_process]

            # This should not raise an error
            result = self.processor.execute_command("kubectl get rollout foo -n bar -o yaml | head -n 40",
                                                     require_approval_check=True)

            # Verify command executed successfully
            self.assertIn("rollout data", result)

    def test_execute_command_dangerous_operators_blocked(self):
        """Test that commands with dangerous operators are blocked."""
        dangerous_commands = [
            "kubectl get pods; rm -rf /",
            "kubectl get pods && kubectl delete pod foo",
            "kubectl get pods > /tmp/out.txt",
            "kubectl get pods $(whoami)",
        ]

        for cmd in dangerous_commands:
            with self.assertRaises(ValueError) as context:
                self.processor.execute_command(cmd, require_approval_check=True)
            self.assertIn("dangerous shell operators", str(context.exception))

    def test_execute_command_unsafe_pipe_blocked(self):
        """Test that unsafe pipe commands are blocked."""
        with self.assertRaises(ValueError) as context:
            self.processor.execute_command("kubectl get pods | bash", require_approval_check=True)

        self.assertIn("Unsafe pipe command", str(context.exception))


if __name__ == '__main__':
    unittest.main()
