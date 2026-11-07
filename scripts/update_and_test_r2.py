"""Install a bundled update into the user's existing R2; preserve .env/runtime data."""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path, PurePosixPath
import shlex
import shutil
import subprocess
import sys
import traceback
import zipfile

EXCLUDE_DIRS = {'.venv','.git','node_modules','__pycache__','Library','data'}

def discover(roots):
    found = set()
    for root in roots:
        if not root.is_dir(): continue
        for folder, dirs, files in os.walk(root, followlinks=False):
            p = Path(folder)
            depth = len(p.relative_to(root).parts)
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in EXCLUDE_DIRS] if depth < 7 else []
            if '.env' in files and 'microbroker' in p.name.lower() and (p/'src/microbroker/cli.py').is_file():
                found.add(p.resolve())
                dirs[:] = []
    return sorted(found)

def select_project(candidates):
    exact = [p for p in candidates if p.name == 'MicroBroker-R2']
    choices = exact or candidates
    if len(choices) == 1: return choices[0]
    if choices:
        print('Birden fazla proje bulundu. Daha önce çalışan R2 klasörünü seç:')
        for i,p in enumerate(choices,1): print(f'{i}. {p}')
        value = input('Klasör numarası: ').strip()
        if value.isdigit() and 1 <= int(value) <= len(choices): return choices[int(value)-1]
        raise RuntimeError('Geçerli bir klasör numarası seçilmedi.')
    value = input('Eski çalışan R2 klasörünü Finder’dan bu pencereye sürükle ve Enter’a bas: ').strip()
    parts = shlex.split(value)
    if len(parts) != 1: raise RuntimeError('Tek bir proje klasörü seçilmeli.')
    return Path(parts[0]).expanduser().resolve()

def install_payload(payload, target):
    if not payload.is_file():
        raise RuntimeError('Paket dosyası yok: '+payload.name+'. BASLAT.command ve kaynak ZIP aynı klasörde bulunmalı.')
    if not (target/'.env').is_file() or not (target/'src/microbroker/cli.py').is_file():
        raise RuntimeError('Seçilen eski projede .env veya proje kodu yok. Çalışan R2 klasörünü seç.')
    plan = []
    with zipfile.ZipFile(payload) as archive:
        for info in archive.infolist():
            if info.is_dir(): continue
            parts = PurePosixPath(info.filename).parts
            if len(parts) < 2 or parts[0] not in {'MicroBroker-R2_1','MicroBroker-R3'} or '..' in parts:
                raise RuntimeError('Güncelleme arşivi geçersiz.')
            relative = Path(*parts[1:])
            # Only known project source/doc files can be replaced, never user/runtime files.
            allowed = parts[1] in {'src','tests','scripts','docs'} or str(relative) in {
                'README.md','CHANGELOG.md','pyproject.toml','requirements-r2.txt'}
            if not allowed: continue
            dest = target/relative
            if dest.is_symlink() or not dest.resolve().is_relative_to(target.resolve()):
                raise RuntimeError('Proje içinde dışarı yönlenen dosya yolu bulundu.')
            plan.append((relative,archive.read(info)))
    if not plan: raise RuntimeError('Güncelleme kodu bulunamadı.')
    backup = target/('R2_Kod_Yedegi_'+datetime.now().strftime('%Y%m%d_%H%M%S_%f'))
    backup.mkdir()
    previous = []
    try:
        for relative,content in plan:
            dest = target/relative
            existed = dest.exists()
            if existed:
                saved = backup/relative
                saved.parent.mkdir(parents=True,exist_ok=True)
                # Copy bytes only. macOS metadata/xattr copying can fail on downloaded files.
                shutil.copyfile(dest,saved)
            previous.append((relative,existed))
            dest.parent.mkdir(parents=True,exist_ok=True)
            dest.write_bytes(content)
    except BaseException as exc:
        restore_failures=[]
        for relative,existed in reversed(previous):
            dest = target/relative
            try:
                if existed: shutil.copyfile(backup/relative,dest)
                else: dest.unlink(missing_ok=True)
            except OSError:
                restore_failures.append(str(relative))
        if restore_failures:
            raise RuntimeError('Bazı dosyalar geri yüklenemedi. Kod yedeği: '+backup.name+
                '; dosyalar: '+', '.join(restore_failures)) from None
        raise
    return backup

def checked(command, project, env):
    result = subprocess.run(command,cwd=project,env=env)
    if result.returncode:
        raise RuntimeError('Bu adım başarısız oldu; sonraki adımlar çalıştırılmadı.')

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--payload',required=True)
    parser.add_argument('--project',type=Path)
    parser.add_argument('--research',action='store_true',help='Validate R3 with stored data instead of a stream test')
    args = parser.parse_args(argv)
    stage='paket ve proje seçimi'
    try:
        if sys.version_info < (3,11): raise RuntimeError('Python 3.11 veya üzeri gerekiyor.')
        payload = Path(args.payload).resolve()
        home = Path.home()
        target = args.project.expanduser().resolve() if args.project else select_project(discover([
            payload.parent.parent, home/'Downloads',home/'Desktop',home/'Documents']))
        if not (target/'.env').is_file(): raise RuntimeError('Eski çalışan projenin .env dosyası bulunamadı.')
        docker = shutil.which('docker')
        if not docker and Path('/Applications/Docker.app/Contents/Resources/bin/docker').is_file():
            docker = '/Applications/Docker.app/Contents/Resources/bin/docker'
        if not docker: raise RuntimeError('Docker bulunamadı; Docker Desktop uygulamasını aç.')
        # Validate existing settings before changing source; never print secret values.
        sys.path.insert(0,str(target/'src'))
        from microbroker.config import Settings
        env = os.environ.copy()
        for key in ('ALPACA_API_KEY','ALPACA_API_SECRET','DATABASE_URL','POSTGRES_PASSWORD','COMPOSE_PROJECT_NAME'):
            env.pop(key,None)
            os.environ.pop(key,None)
        settings = Settings.from_env(str(target/'.env'))
        if not args.research and (not settings.api_key or not settings.api_secret):
            raise RuntimeError('Seçilen eski .env dosyasında çalışan Alpaca anahtarları yok.')
        if not settings.database_url.startswith('postgresql://'):
            raise RuntimeError('Seçilen eski projede PostgreSQL ayarı yok; çalışan R2 klasörünü seç.')
        python = target/'.venv/bin/python3'
        stage='Python ortamı'
        if not python.is_file():
            checked([sys.executable,'-m','venv',str(target/'.venv')],target,env)
        print(f'Çalışan proje: {target}',flush=True)
        stage='kod yedekleme ve güncelleme'
        backup = install_payload(payload,target)
        print(f'Kod güncellendi. Önceki kodun yedeği: {backup.name}',flush=True)
        env['PYTHONPATH'] = str(target/'src')
        stage='bağımlılık kontrolü'
        probe = subprocess.run([str(python),'-c','import psycopg, websockets; from websockets.sync.client import connect'],cwd=target,env=env,capture_output=True)
        if probe.returncode:
            checked([str(python),'-m','pip','install','--timeout','60','--retries','3','-r','requirements-r2.txt'],target,env)
        stage='yapılandırma kontrolü'
        checked([str(python),'-m','microbroker','doctor'],target,env)
        stage='PostgreSQL konteynerini başlatma'
        checked([docker,'compose','--env-file','.env','up','-d','--wait','postgres'],target,env)
        stage='veritabanı şeması'
        checked([str(python),'-m','microbroker','init-db'],target,env)
        stage='PostgreSQL doğrulaması'
        checked([str(python),'-m','microbroker','verify-postgres'],target,env)
        if args.research:
            stage='R3 veri ve tekrar oynatma doğrulaması'
            checked([str(python),'-m','microbroker','verify-research','--symbols','AAPL,MSFT','--feed','iex'],target,env)
            print('R3 yerel veri ve tekrar oynatma kontrolü tamamlandı. Son JSON çıktısını paylaş.',flush=True)
            return 0
        print('\nKESİNTİ TESTİ BAŞLIYOR: İlk verilerden yaklaşık 20 saniye sonra interneti kapat; 90 saniye sonra aç. Test 5 dakika sürer. Son JSON çıktısını paylaş.\n',flush=True)
        checked([str(python),'-m','microbroker','stream-market-data','--symbols','AAPL,MSFT','--feed','iex','--duration','300'],target,env)
        return 0
    except KeyboardInterrupt:
        print('Kullanıcı tarafından durduruldu.')
        return 130
    except Exception as exc:
        # Configuration errors may contain lines from .env, so only our fixed messages are shown.
        print('Durulan adım: '+stage,file=sys.stderr)
        if type(exc) is RuntimeError:
            print(str(exc),file=sys.stderr)
        else:
            print('Hata türü: '+type(exc).__name__,file=sys.stderr)
            if isinstance(exc,OSError):
                print('İşletim sistemi hata kodu: '+str(exc.errno),file=sys.stderr)
                if exc.filename:print('Dosya: '+Path(exc.filename).name,file=sys.stderr)
            frames=traceback.extract_tb(exc.__traceback__)
            if frames:print('Kod konumu: '+Path(frames[-1].filename).name+':'+str(frames[-1].lineno),file=sys.stderr)
            print('Bu hata çıktısını paylaş. Anahtarlarını gönderme.',file=sys.stderr)
        return 1

if __name__ == '__main__': raise SystemExit(main())
