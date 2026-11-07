# MicroBroker kullanım ve yayın rehberi

## 1. Anahtarsız demo

Python 3.11 veya üstüyle, proje klasöründe:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install .
microbroker verify-event-study
microbroker demo-event-study --output data/demo
cat data/demo/report.txt
```

İlk doğrulama 19 sentetik kontrol çalıştırır. Demo pozitif, negatif, nötr ve elenen analiz örnekleriyle rapor üretir. Aynı çıktı klasörünü tekrar kullanma; uygulama mevcut raporu ezmez.

## 2. İsteğe bağlı: PostgreSQL ve Alpaca ile araştırma

```bash
python -m pip install '.[postgres,stream,analysis]'
cp -n .env.example .env
chmod 600 .env
```

`.env` dosyasını editörde aç. Alpaca Paper anahtarlarını kendi ortamında gir. Mevcut bir MicroBroker veritabanın varsa çalışan `DATABASE_URL` satırını kullan ve mevcut PostgreSQL konteynerini başlat. İlk kez kuruyorsan örnekteki `POSTGRES_PASSWORD` ve `DATABASE_URL` değerlerini eşleştir, ardından `docker compose up -d postgres` çalıştır. Compose servisi PostgreSQL 16'yı yerel 5432 portuna bağlar.

```bash
microbroker doctor --require-postgres --require-credentials
microbroker verify-postgres
microbroker init-db
microbroker prepare-finbert --model-dir models/finbert
```

`doctor` yapılandırmanın biçimini denetler. `verify-postgres` gerçek veritabanına bağlanıp geçici şemada kontrollü testler çalıştırır. `prepare-finbert` model dosyalarını indirir; bunlar yerel `models/` klasöründe kalır.

Piyasa açıkken, başka bir MicroBroker akışı çalışmıyorsa 90 dakikalık emirsiz veri toplama:

```bash
microbroker run-event-study-session \
  --symbols AAPL,MSFT,NVDA,TSLA \
  --feed iex \
  --duration 5400 \
  --poll-seconds 30 \
  --lookback-minutes 10 \
  --model-dir models/finbert
```

SPY fiyatları otomatik eklenir. Toplama tamamlandığında:

```bash
microbroker event-study --latest --format text --output data/event-study-r30.json
```

Geç gelen haberler için özellikle +60 dakika sonucu eksik olabilir. Raporun fiyat referansı, örnek seçimi ve eksik veri kuralları [yöntem belgesinde](EVENT_STUDY.md) açıklanır.

## 3. GitHub'a yükleme

Temiz ZIP'i ayrı bir klasöre çıkar. Eski çalışma klasöründen `.env`, veritabanı, model veya oturum çıktısı kopyalamadan bu klasörü kullan. GitHub CLI kuruluysa terminalde:

```bash
gh auth login --hostname github.com --git-protocol https --web --scopes workflow
gh auth setup-git
git init -b main
git add .
git diff --cached --stat
git commit -m "Publish MicroBroker R30 portfolio release"
gh repo create microbroker --private --source=. --remote=origin --push
```

Git kimliğin yapılandırılmamışsa `git commit` adımından önce GitHub hesabında kullanmak istediğin ad ve e-posta adresini bu depoda `git config user.name` ve `git config user.email` ile ayarla. `microbroker` adında mevcut bir depon varsa oluşturma adımını çalıştırma; mevcut deponun içeriğini ve hedef dalını kontrol et. İlk yüklemeden sonra GitHub'daki **Actions** sayfasından CI sonucuna bak. Depoyu portföyünde herkese açmak istersen GitHub ayarlarından görünürlüğünü değiştir.
