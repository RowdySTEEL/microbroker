import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile
import contextlib
import io
import errno
from types import SimpleNamespace

spec=importlib.util.spec_from_file_location('launcher',Path(__file__).resolve().parents[1]/'scripts/update_and_test_r2.py')
launcher=importlib.util.module_from_spec(spec);spec.loader.exec_module(launcher)

class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.target=self.root/'MicroBroker-R2';self.target.mkdir()
        (self.target/'src/microbroker').mkdir(parents=True)
        (self.target/'src/microbroker/cli.py').write_text('old code')
        (self.target/'.env').write_text('SECRET=preserve')
        (self.target/'docker-compose.yml').write_text('existing compose')
        (self.target/'data').mkdir();(self.target/'data/db').write_bytes(b'original DB')
        self.payload=self.root/'payload.zip'
    def tearDown(self): self.temp.cleanup()
    def archive(self,files):
        with zipfile.ZipFile(self.payload,'w') as z:
            for name,content in files.items():z.writestr(name,content)
    def test_updates_source_and_preserves_runtime_and_compose(self):
        self.archive({'MicroBroker-R2_1/src/microbroker/cli.py':'new code',
                      'MicroBroker-R2_1/.env':'do not copy',
                      'MicroBroker-R2_1/docker-compose.yml':'do not copy',
                      'MicroBroker-R2_1/data/db':'do not copy'})
        backup=launcher.install_payload(self.payload,self.target)
        self.assertEqual((backup/'src/microbroker/cli.py').read_text(),'old code')
        self.assertEqual((self.target/'src/microbroker/cli.py').read_text(),'new code')
        self.assertEqual((self.target/'.env').read_text(),'SECRET=preserve')
        self.assertEqual((self.target/'data/db').read_bytes(),b'original DB')
        self.assertEqual((self.target/'docker-compose.yml').read_text(),'existing compose')
    def test_discovery_requires_env_and_code_and_skips_venv(self):
        broken=self.root/'MicroBroker-R2_1';broken.mkdir();(broken/'.env').write_text('empty')
        self.assertEqual(launcher.discover([self.root]),[self.target.resolve()])
    def test_rejects_archive_traversal_before_writing(self):
        self.archive({'MicroBroker-R2_1/src/microbroker/cli.py':'new', 'MicroBroker-R2_1/../other':'bad'})
        with self.assertRaises(RuntimeError):launcher.install_payload(self.payload,self.target)
        self.assertEqual((self.target/'src/microbroker/cli.py').read_text(),'old code')
    def test_partial_write_failure_restores_prior_code(self):
        self.archive({'MicroBroker-R2_1/src/microbroker/cli.py':'new', 'MicroBroker-R2_1/README.md':'new'})
        original=Path.write_bytes
        def fail(path,content):
            if path.name=='README.md':raise OSError('injected')
            return original(path,content)
        with patch.object(Path,'write_bytes',fail),self.assertRaises(OSError):
            launcher.install_payload(self.payload,self.target)
        self.assertEqual((self.target/'src/microbroker/cli.py').read_text(),'old code')
        self.assertFalse((self.target/'README.md').exists())
    def test_rejects_symlink_outside_project(self):
        outside=self.root/'outside';outside.mkdir()
        (self.target/'docs').symlink_to(outside,target_is_directory=True)
        self.archive({'MicroBroker-R2_1/docs/file.md':'bad'})
        with self.assertRaises(RuntimeError):launcher.install_payload(self.payload,self.target)
        self.assertFalse((outside/'file.md').exists())
    def test_missing_payload_explains_same_folder_requirement(self):
        with self.assertRaisesRegex(RuntimeError,'aynı klasörde'):
            launcher.install_payload(self.payload,self.target)
    def test_update_does_not_depend_on_metadata_copy(self):
        self.archive({'MicroBroker-R3/src/microbroker/cli.py':'new'})
        with patch.object(launcher.shutil,'copy2',side_effect=PermissionError('metadata test')):
            backup=launcher.install_payload(self.payload,self.target)
        self.assertEqual((backup/'src/microbroker/cli.py').read_text(),'old code')
        self.assertEqual((self.target/'src/microbroker/cli.py').read_text(),'new')
    def test_safe_os_error_diagnostics(self):
        (self.target/'.env').write_text('DATABASE_URL=postgresql://local/db\n')
        capture=io.StringIO()
        with patch.object(launcher.shutil,'which',return_value='/usr/bin/docker'),patch.object(launcher.subprocess,'run',return_value=SimpleNamespace(returncode=0)),patch.object(launcher,'install_payload',side_effect=PermissionError(errno.EACCES,'TEST_SECRET',str(self.target/'README.md'))),contextlib.redirect_stderr(capture),contextlib.redirect_stdout(io.StringIO()):
            result=launcher.main(['--payload',str(self.payload),'--project',str(self.target),'--research'])
        self.assertEqual(result,1)
        self.assertIn('PermissionError',capture.getvalue())
        self.assertIn('13',capture.getvalue())
        self.assertIn('README.md',capture.getvalue())
        self.assertNotIn('TEST_SECRET',capture.getvalue())
