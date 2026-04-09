# GlobalLag

**공시 시차 시각화 — Disclosure Timing Gap Visualization**

한국 기업의 해외 활동이 한국(DART) vs 해외 레지스트리(Companies House, INPI 등)에 얼마나 시차를 두고 공개되는지 시각화하는 Scrollytelling 프로젝트.

## 🚀 Quick Start

```bash
npm install
npm run dev
```

## 📊 5-Scene Scrollytelling

1. **Global Footprint** — D3 globe with 3,046 subsidiaries
2. **Timeline Race** — 4-channel disclosure arrival (SEC → DART → Media → Research)
3. **Gap Distribution** — Histogram of timing gaps
4. **Who Saw It First** — Racing visualization
5. **Regulatory Structure** — Country-by-country comparison

## 🗂️ Project Structure

```
global-lag/
├── frontend/          (Vite + Scrollama + D3)
├── scripts/           (Data sync & analysis)
├── data/              (Supabase exports)
└── docs/              (기획안, 스키마)
```

## 📚 Documentation

- [`docs/기획안.md`](docs/기획안.md) — Full project specification
- [`docs/data_schema.md`](docs/data_schema.md) — Supabase schema design

## 📦 Data Sources

- **DART**: 100 KOSPI companies, 3,046 subsidiaries (88.8% classified)
- **Companies House**: 21 UK matches
- **INPI**: 195 French companies
- **OpenCorporates**: Global registry (coming soon)

## ⚖️ License & Attribution

Data sources require proper attribution. See [DATA_SOURCES_LICENSE.md](../news-epoch/DATA_SOURCES_LICENSE.md) in news-epoch repo.

---

**Related Project**: [news-epoch](https://github.com/Indexnlog/news-epoch)
