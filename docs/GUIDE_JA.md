# Data Space Grid 解説書

電力セクター向けフェデレーテッド・データスペースの設計と実装に関する技術解説書。

---

## 目次

1. [背景と課題](#1-背景と課題)
2. [データスペースとは何か](#2-データスペースとは何か)
3. [5層アーキテクチャ](#3-5層アーキテクチャ)
4. [参加者とその役割](#4-参加者とその役割)
5. [セマンティックモデル（業界標準データ定義）](#5-セマンティックモデル業界標準データ定義)
6. [コネクタ：データ交換の守門人](#6-コネクタデータ交換の守門人)
7. [契約交渉の仕組み](#7-契約交渉の仕組み)
8. [認証・認可の二重防御](#8-認証認可の二重防御)
9. [プライバシーと同意管理](#9-プライバシーと同意管理)
10. [監査証跡（Audit Trail）](#10-監査証跡audit-trail)
11. [ユースケース1：配電系統の混雑管理](#11-ユースケース1配電系統の混雑管理)
12. [ユースケース2：系統トポロジと電圧制御](#12-ユースケース2系統トポロジと電圧制御)
13. [セキュリティモデル詳解](#13-セキュリティモデル詳解)
14. [技術スタック詳解](#14-技術スタック詳解)
15. [デモの実行方法](#15-デモの実行方法)
16. [用語集](#16-用語集)

---

## 1. 背景と課題

### 電力系統のデータ課題

従来の電力系統では、配電事業者（DSO）が系統運用データを一元管理していた。しかし、再生可能エネルギー・EV・蓄電池の普及に伴い、以下の課題が顕在化している：

| 課題 | 具体例 |
|------|--------|
| **データのサイロ化** | DSO、アグリゲーター、需要家がそれぞれ別のシステムでデータを保有し、横断的な活用ができない |
| **プライバシーの懸念** | スマートメーターデータは生活パターンを暴露する。需要家の同意なく共有すべきではない |
| **信頼の欠如** | 事業者間でデータを共有する際、誰がどう使うか保証がない |
| **相互運用性** | CIM、IEC 61850、OpenADR など規格が乱立し、データ形式が統一されていない |
| **リアルタイム性** | 系統混雑や電圧逸脱は即座に対処が必要だが、契約交渉に時間がかかる |

### なぜ「API統合」では不十分か

単純なAPI連携（REST API を公開して相手に呼んでもらう）では以下が担保されない：

- **データ主権**: 一度データを渡したら、相手の使い方を制御できない
- **利用目的の制限**: 「混雑管理」のために渡したデータが「マーケティング」に転用される可能性
- **監査可能性**: 誰がいつ何のデータにアクセスしたか追跡できない
- **同意の即時撤回**: 需要家が同意を取り消しても、既に渡したデータは戻せない

**フェデレーテッド・データスペース**はこれらの課題を構造的に解決する。

---

## 2. データスペースとは何か

### 定義

データスペースとは、**データの主権（所有権と管理権）を各参加者が保持したまま**、信頼された仕組みを通じてデータを共有・交換するための分散型エコシステムである。

### 基本原則

```
┌─────────────────────────────────────────────────────────────┐
│                  Federated Data Space                        │
│                                                             │
│  原則1: データ主権の保持                                      │
│    → データは元のノードに留まる。共有されるのはメタデータ        │
│                                                             │
│  原則2: 契約ベースのアクセス                                  │
│    → 全てのデータ交換に機械実行可能な契約が必要                 │
│                                                             │
│  原則3: 目的制限                                             │
│    → データの利用目的を契約で明示し、逸脱を技術的に防止          │
│                                                             │
│  原則4: 最小開示                                             │
│    → 必要最小限の情報のみを、適切な匿名化を施して共有           │
│                                                             │
│  原則5: 完全監査                                             │
│    → 全てのデータ交換をハッシュ付きで記録、改ざん検出可能        │
└─────────────────────────────────────────────────────────────┘
```

### 欧州の先行事例

本プロジェクトは以下の欧州イニシアチブの設計思想を参考にしている：

- **GAIA-X**: 欧州クラウド・データインフラの信頼フレームワーク
- **IDS (International Data Spaces)**: 産業データ共有のリファレンスアーキテクチャ
- **IDSA Connector**: データスペース参加者間の信頼仲介コンポーネント

---

## 3. 5層アーキテクチャ

本システムは5つの層で構成される。各層が独立した責務を持ち、上位層は下位層のサービスに依存する。

![Architecture](../demos/concept-architecture.gif)

### Layer 1: Identity / Trust（認証・信頼基盤）

**責務**: 「あなたは誰か？」「信頼できるか？」を保証する。

| 技術 | 用途 |
|------|------|
| **Keycloak OIDC** | 組織・ユーザーの認証。JWT アクセストークンを発行 |
| **mTLS** | サービス間の相互TLS認証。証明書で身元を暗号的に保証 |
| **JWK キャッシュ** | Keycloak の公開鍵をローカルにキャッシュし、トークン検証を高速化 |

```
Aggregator                    Keycloak                     DSO
    │                            │                          │
    │── client_credentials ─────>│                          │
    │<── JWT access_token ───────│                          │
    │                            │                          │
    │── Bearer <JWT> + mTLS cert ──────────────────────────>│
    │                            │     ← JWK cached locally │
    │                            │     トークン検証（ローカル）│
    │<──────────────────────── 200 OK ──────────────────────│
```

**なぜ2つの認証が必要か？**
- **OIDC (JWT)**: 「この要求はGreenFlexアグリゲーターの管理者が行った」を証明
- **mTLS**: 「このTCP接続は確かにGreenFlexのサーバーから来ている」を暗号的に保証

どちらか一方だけでは不十分。JWTだけではトークン窃取リスクがあり、mTLSだけでは組織内の誰がアクセスしたか特定できない。

#### 実装ファイル

| ファイル | 内容 |
|---------|------|
| `src/connector/auth.py` | JWK キャッシュ、JWT検証、mTLS証明書DN抽出 |
| `infrastructure/keycloak/realm-export.json` | Keycloak レルム設定（クライアント、ロール） |
| `infrastructure/certs/generate-dev-certs.sh` | 開発用CA + 各ノード証明書の生成 |

---

### Layer 2: Semantic Model（セマンティックモデル）

**責務**: 「データの意味は何か？」を統一する。

電力業界には複数の標準規格がある。本システムでは以下の3つを Pydantic モデルとして実装：

| 規格 | 対象 | 本システムでのモデル |
|------|------|---------------------|
| **CIM** (Common Information Model) | 系統トポロジ、フィーダー制約 | `FeederConstraint`, `CongestionSignal`, `HostingCapacity`, `GridNode` |
| **IEC 61850** | DER（分散電源）の柔軟性 | `FlexibilityEnvelope`, `DERUnit`, `PQRange`, `StateOfCharge` |
| **OpenADR** | デマンドレスポンスイベント | `DREvent`, `DRSignal`, `DispatchCommand`, `DispatchActual` |
| **独自** | 需要家データ | `DemandProfile`, `ConsentRecord`, `AnonymizedLoadSeries` |

#### 感度分類（Sensitivity Tier）

全てのデータモデルに `sensitivity` フィールドがあり、3段階で分類される：

| Tier | 対象データ | アクセス可能なロール |
|------|-----------|---------------------|
| `HIGH` | 系統トポロジ、保護設定 | `dso_operator` のみ |
| `MEDIUM` | 混雑信号、柔軟性エンベロープ | `dso_operator`, `aggregator` |
| `HIGH_PRIVACY` | スマートメーター、BEMS データ | 同意が必要、目的別匿名化必須 |

```python
# src/semantic/cim.py
class FeederConstraint(BaseModel):
    feeder_id: str
    max_active_power_kw: float
    congestion_level: float          # 0.0〜1.0
    sensitivity: SensitivityTier = SensitivityTier.MEDIUM  # ← 全モデルに存在
```

#### 実装ファイル

| ファイル | 内容 |
|---------|------|
| `src/semantic/cim.py` | CIM系統モデル（7クラス） |
| `src/semantic/iec61850.py` | DER柔軟性モデル（8クラス） |
| `src/semantic/openadr.py` | DRイベントモデル（6クラス） |
| `src/semantic/consumer.py` | 需要家データモデル（5クラス） |

---

### Layer 3: Catalog / Discovery（カタログ・発見）

**責務**: 「どんなデータがあるか？」「誰が提供しているか？」を発見する。

![Catalog](../demos/concept-catalog.gif)

フェデレーテッドカタログは **メタデータのみ** を管理する。実データは各ノードに留まる。

```
DSO                     Federated Catalog                Aggregator
 │                            │                              │
 │── POST /assets ───────────>│                              │
 │   {provider: "dso-001",    │                              │
 │    type: "feeder_constraint"│                             │
 │    sensitivity: "medium",   │                             │
 │    endpoint: "https://..."}│                              │
 │                            │                              │
 │                            │<── GET /assets?type=... ─────│
 │                            │── 結果 ─────────────────────>│
 │                            │   [{asset_id, policy, ...}]  │
 │                            │                              │
 │   ※ 実データはDSOノードに   │   ※ カタログにはメタデータ     │
 │     留まっている           │     のみ格納                  │
```

#### カタログAPIエンドポイント

| メソッド | パス | 説明 |
|---------|------|------|
| `POST` | `/api/v1/assets` | データアセットを登録 |
| `GET` | `/api/v1/assets` | アセットを検索（provider, type, sensitivity で絞り込み） |
| `GET` | `/api/v1/assets/{id}` | アセット詳細（ポリシー情報含む） |
| `DELETE` | `/api/v1/assets/{id}` | アセットを登録解除 |
| `POST` | `/api/v1/contracts` | 契約交渉を開始 |
| `PUT` | `/api/v1/contracts/{id}/accept` | 契約を承認 |
| `PUT` | `/api/v1/contracts/{id}/reject` | 契約を拒否 |

#### 実装ファイル

| ファイル | 内容 |
|---------|------|
| `src/catalog/main.py` | FastAPI アプリ |
| `src/catalog/routes.py` | API ルーティング |
| `src/catalog/store.py` | SQLite ストア（アセット・契約テーブル） |
| `src/catalog/schemas.py` | リクエスト/レスポンスの Pydantic スキーマ |
| `src/connector/catalog_client.py` | カタログにアクセスするクライアントライブラリ |

---

### Layer 4: Policy / Contract / Consent（ポリシー・契約・同意）

**責務**: 「このデータにアクセスしていいか？」「どの条件で？」を判定する。

この層は3つのサブコンポーネントから成る：

#### 4a. 契約交渉（Contract Negotiation）

![Contract](../demos/concept-contract-states.gif)

全てのデータ交換に **ACTIVE な契約** が必要。契約は状態遷移マシンで管理される：

```
              negotiate()        accept()
  OFFERED ──────────────> NEGOTIATING ──────────> ACTIVE
     │                       │                     │  │
     │ reject()              │ reject()            │  │
     ↓                       ↓                     │  │
  REJECTED               REJECTED              expire() revoke()
  (端末状態)             (端末状態)                │  │
                                                   ↓  ↓
                                               EXPIRED  REVOKED
                                              (端末状態) (端末状態)
```

契約に含まれる条件：

| フィールド | 説明 | 例 |
|-----------|------|-----|
| `purpose` | データの利用目的 | `"congestion_management"` |
| `allowed_operations` | 許可される操作 | `["read"]` |
| `redistribution_allowed` | 第三者への再配布 | `false` |
| `retention_days` | データ保持期間 | `30` |
| `anonymization_required` | 匿名化が必要か | `true` |
| `emergency_override` | 緊急時のDSOアクセス | `true` |
| `valid_from` / `valid_until` | 有効期間 | `2026-08-15` 〜 `2026-11-15` |

#### 4b. ポリシーエンジン（Policy Engine）

ポリシーエンジンは以下の順序でアクセスを評価する：

```
リクエスト受信
     │
     ↓
[1] 緊急オーバーライド判定
     │ → DSOオペレーターかつ契約にemergency_override=true → 許可
     │
     ↓
[2] 契約の有効性チェック
     │ → ステータスがACTIVE？有効期間内？アセットID一致？消費者ID一致？
     │
     ↓
[3] 利用目的チェック
     │ → リクエストの目的が契約の目的と一致するか？
     │
     ↓
[4] 感度ティアチェック
     │ → 要求者のロールがデータの感度ティアにアクセス可能か？
     │
     ↓
[5] 再配布・保持制限チェック
     │ → 再配布を要求していないか？保持期間は契約内か？
     │
     ↓
  許可 or 拒否（理由付き）
```

#### 4c. 同意管理（Consent Manager）

需要家データは **同意（Consent）** なしにアクセスできない：

```python
# 同意を付与
consent = consent_mgr.grant_consent(
    purpose="research",       # 利用目的
    requester_id="agg-001",   # 要求者
    expiry=...,               # 有効期限
)

# 同意をチェック
has_consent = consent_mgr.check_consent("agg-001", "research")  # True

# 同意を即時撤回
consent_mgr.revoke_consent(consent.consent_id)
has_consent = consent_mgr.check_consent("agg-001", "research")  # False（即時）
```

#### 実装ファイル

| ファイル | 内容 |
|---------|------|
| `src/connector/contract.py` | 契約状態遷移マシン（`ContractManager`） |
| `src/connector/policy.py` | ポリシーエンジン（`PolicyEngine`） |
| `src/connector/models.py` | `DataUsageContract`, `PolicyRule`, `ContractOffer` |
| `src/participants/prosumer/consent.py` | 同意管理（`ConsentManager`） |

---

### Layer 5: Access / Exchange（アクセス・交換）

**責務**: 実際のデータ転送。同期（REST API）と非同期（Kafka）の2チャネル。

| チャネル | 用途 | 例 |
|---------|------|-----|
| **REST API** | 同期的なデータ取得・登録 | `GET /constraints`, `POST /flexibility-offers` |
| **Kafka** | 非同期のイベント・コマンド | DR ディスパッチ、混雑アラート、テレメトリ |

#### Kafka トピック

| トピック | 送信者 | 受信者 | 内容 |
|---------|--------|--------|------|
| `dr-events` | DSO | Aggregator, Prosumer | DRイベント通知 |
| `dispatch-commands` | DSO | Aggregator | リアルタイムディスパッチ指令 |
| `dispatch-actuals` | Aggregator | DSO | ディスパッチ結果報告 |
| `congestion-alerts` | DSO | Aggregator | 混雑レベルのリアルタイム変化 |
| `audit-events` | 全ノード | 監査サービス | 監査エントリ（集中分析用） |

#### 各参加者ノードのAPIエンドポイント

**DSO (Port 8001)**

| メソッド | パス | 説明 |
|---------|------|------|
| `GET` | `/api/v1/constraints` | フィーダー制約（契約必須） |
| `GET` | `/api/v1/congestion-signals` | 混雑信号 |
| `GET` | `/api/v1/hosting-capacity` | ホスティング容量 |
| `POST` | `/api/v1/flexibility-requests` | 柔軟性要求の発行 |

**Aggregator (Port 8002)**

| メソッド | パス | 説明 |
|---------|------|------|
| `GET` | `/api/v1/flexibility-offers` | 柔軟性エンベロープ |
| `POST` | `/api/v1/flexibility-offers` | 柔軟性オファー提出 |
| `POST` | `/api/v1/dispatch-response` | ディスパッチ実績報告 |

**Prosumer (Port 8003)**

| メソッド | パス | 説明 |
|---------|------|------|
| `GET` | `/api/v1/demand-profile` | 需要プロファイル（同意+目的必須） |
| `GET` | `/api/v1/controllable-margin` | 制御可能マージン |
| `POST` | `/api/v1/consents` | 同意を付与 |
| `DELETE` | `/api/v1/consents/{id}` | 同意を撤回 |

---

## 4. 参加者とその役割

### 参加者間のデータ主権

**最も重要な設計原則**: 各参加者は自分のデータの主権を保持する。

```
┌──────────────────────────────────────────────────────────────────┐
│                                                                  │
│  DSO が見えるもの              Aggregator が見えるもの             │
│  ─────────────────            ──────────────────────             │
│  • 系統トポロジ                • 自社フリートの柔軟性              │
│  • フィーダー制約              • ディスパッチ指令（契約経由）        │
│  • 電圧・混雑情報              • 契約で許可されたDSOデータ          │
│  • 全柱の集約負荷              │                                  │
│                                                                  │
│  ✗ 個別住宅のデータ            ✗ 系統トポロジ                     │
│  ✗ 需要家の生データ            ✗ 個別DERの状態                    │
│                                                                  │
│                Prosumer が見えるもの                               │
│                ─────────────────────                              │
│                • 自分のメーターデータ（生）                         │
│                • 自分のPV/EV/蓄電池の状態                          │
│                • 同意ダッシュボード                                │
│                                                                  │
│                ✗ 隣の家のデータ                                    │
│                ✗ 系統トポロジ                                      │
│                ✗ アグリゲーターの内部データ                         │
└──────────────────────────────────────────────────────────────────┘
```

---

## 5. セマンティックモデル（業界標準データ定義）

### CIM（Common Information Model）による系統モデリング

CIMは電力系統のデータ交換のための国際標準（IEC 61970/61968）。本システムでは以下をモデル化：

```
Substation (66/6.6kV)
  └── Feeder (F-101)
        ├── GridNode (substation端)
        │     └── Switch (開閉器)
        ├── GridNode (柱上変圧器)
        │     └── FeederConstraint (容量制約)
        ├── ...
        └── GridNode (末端)
              └── HostingCapacity (受入容量)
```

**FeederConstraint の例:**

```python
FeederConstraint(
    feeder_id="F-101",
    max_active_power_kw=5000,    # 定格容量 5MW
    min_voltage_pu=0.95,         # 下限電圧 0.95pu
    max_voltage_pu=1.05,         # 上限電圧 1.05pu
    congestion_level=0.92,       # 現在の混雑率 92%
    sensitivity=SensitivityTier.MEDIUM,
)
```

### IEC 61850 による DER 柔軟性モデリング

アグリゲーターは個別のDER（EV、蓄電池、HP等）の状態を **公開しない**。代わりに、フリート全体の **柔軟性エンベロープ** を集約して共有する：

```
  個別DER (非公開)                     柔軟性エンベロープ (共有)
  ──────────────                      ────────────────────────
  EV-001: 充電中 6kW                   方向: DOWN (負荷削減)
  EV-002: 待機                         P範囲: -966 〜 0 kW
  Batt-003: SOC 78%           →       信頼度: HIGH (92%)
  Batt-004: SOC 45%                   デバイス構成:
  HP-005: 運転中 2.5kW                   EV充電器: 45%
  ...                                    蓄電池: 20%
  (1000台の詳細)                          AC: 25%
                                         ヒートポンプ: 10%
```

### OpenADR によるDRイベント

DSO からアグリゲーターへのディスパッチ指令：

```python
DispatchCommand(
    command_id="dispatch-001",
    feeder_id="F-101",
    target_power_kw=236,           # 236kW の負荷削減要求
    activation_time=NOW + 15min,   # 15分後に開始
    duration_minutes=300,          # 5時間
    ramp_rate_kw_per_min=7.9,      # 30分でランプアップ
    is_emergency=False,
)
```

---

## 6. コネクタ：データ交換の守門人

**Data Space Connector** は全参加者ノードが共有するライブラリ。全てのAPIリクエストをインターセプトし、認証・ポリシー・監査を自動適用する。

```
外部リクエスト
     │
     ↓
┌─────────────────────────────────────────┐
│        ConnectorMiddleware               │
│                                         │
│  [1] 認証 (Auth)                        │
│      JWT検証 + mTLS証明書チェック         │
│           │                             │
│  [2] ポリシー (Policy)                   │
│      契約チェック + 目的チェック           │
│      + 感度ティアチェック                │
│           │                             │
│  [3] ルートハンドラに転送                │
│           │                             │
│  [4] 監査 (Audit)                       │
│      req_hash + resp_hash を記録        │
│           │                             │
│  [5] レスポンス返却                     │
└─────────────────────────────────────────┘
     │
     ↓
外部レスポンス
```

**重要**: 認証・ポリシー・監査はオプションではない。ミドルウェアを経由しないデータ交換は存在しない。

```python
# src/participants/dso/main.py — 全ノードでこのパターン
app = FastAPI(title="DSO Node")
app.add_middleware(
    ConnectorMiddleware,
    auth_backend=KeycloakAuthBackend(),
    audit_logger=AuditLogger(),
    participant_id="dso-001",
)
```

---

## 7. 契約交渉の仕組み

### 契約のライフサイクル

```
1. Aggregator がカタログで DSO の「フィーダー制約」アセットを発見

2. Aggregator が契約を提案（Offer）
   → purpose: "congestion_management"
   → operations: ["read"]
   → retention: 30日
   → emergency_override: true

3. DSO が条件を確認し、交渉（Negotiate）
   → 条件の修正があればここで行う

4. DSO が承認（Accept）
   → 契約ステータス: ACTIVE

5. Aggregator がデータにアクセス可能に
   → コネクタが契約の有効性を毎リクエスト検証

6. 期限到来 or 当事者の意思で終了
   → EXPIRED or REVOKED
```

### 契約なしでのアクセス試行

```python
# spy がアクセスを試みる（契約は OFFERED 状態のまま）
decision = policy_engine.evaluate(
    requester_id="spy-001",
    asset_id="asset-constraint-f101",
    contract=fake_contract,        # ステータス: OFFERED（ACTIVEではない）
    purpose="espionage",
    operation="read",
)
# → allowed=False
# → reason="contract is not active (status=offered)"
# → 監査ログに "denied" として記録される
```

---

## 8. 認証・認可の二重防御

![Auth](../demos/concept-auth-flow.gif)

### 認証失敗パターン

| 状況 | HTTPレスポンス | 監査記録 |
|------|---------------|---------|
| トークンなし | `401 Unauthorized` + `WWW-Authenticate` | denied |
| 期限切れトークン | `401 Unauthorized` (expired) | denied |
| 不正なトークン | `401 Unauthorized` (invalid signature) | denied |
| 未登録の参加者 | `403 Forbidden` (not registered) | denied |
| 感度ティア違反 | `403 Forbidden` (tier violation) | denied |
| 契約なし | `403 Forbidden` (no active contract) | denied |
| 目的不一致 | `403 Forbidden` (purpose mismatch) | denied |
| **全条件クリア** | **`200 OK`** | **success** |

### `/health` エンドポイントの例外

全ノードの `/health` は認証なしでアクセス可能（インフラ監視用）。

---

## 9. プライバシーと同意管理

![Privacy](../demos/concept-privacy.gif)

### 目的別開示レベル

需要家データは **要求の目的** に応じて自動的に変換される：

| 目的 | 開示レベル | 出力内容 |
|------|-----------|---------|
| `research` | **AGGREGATED** | 統計値のみ（平均、標準偏差、ピーク）。個人特定不可 |
| `dr_dispatch` | **CONTROLLABILITY** | 制御可能マージン（kW）のスカラー値1つだけ |
| `billing` | **IDENTIFIED** | 明示的同意のもとで身元付きデータ |
| `forecasting` | **ANONYMIZED** | k-匿名化（k=5）。IDを置換、時系列は保持 |
| `grid_analysis` | **AGGREGATED** | 統計値のみ |

### 匿名化パイプライン

```
[生メーターデータ]
       │
       ↓
  [同意チェック] ─── 同意なし → 403 拒否
       │
       ↓
  [目的解決] → 目的から開示レベルを決定
       │
       ↓
  [匿名化エンジン]
       │
       ├── AGGREGATED → 統計のみ、IDなし
       ├── ANONYMIZED → k-匿名化、IDハッシュ化
       ├── CONTROLLABILITY → スカラー値のみ
       └── IDENTIFIED → 同意付きで身元保持
       │
       ↓
  [開示データ]（元の生データとは異なる）
```

### k-匿名化の保証

匿名化されたデータは、少なくとも `k-1` 個の他のレコードと区別がつかないことが保証される。デフォルト `k=5`。

### 同意の即時撤回

```
時刻 T=0:  Prosumer が research 目的の同意を付与
時刻 T=1:  Aggregator が demand-profile を取得 → 成功（統計値）
時刻 T=2:  Prosumer が同意を撤回
時刻 T=3:  Aggregator が同じリクエスト → 403 拒否（即時反映）
```

---

## 10. 監査証跡（Audit Trail）

![Audit](../demos/concept-audit.gif)

### 監査エントリの構造

全てのデータ交換が以下の情報と共に記録される：

```json
{
  "timestamp": "2026-08-15T16:33:00Z",
  "requester_id": "agg-001",
  "provider_id": "dso-001",
  "asset_id": "/api/v1/constraints",
  "purpose_tag": "congestion_management",
  "contract_id": "c-a7b3e...",
  "request_hash": "sha256:9f4a2b8c71d3e...",
  "response_hash": "sha256:e1c8d3f5a902b...",
  "action": "read",
  "outcome": "success"
}
```

### SHA-256 ハッシュによる改ざん検出

リクエストとレスポンスの本文をそれぞれ SHA-256 でハッシュ化して記録。後からデータが改ざんされた場合、ハッシュの不一致で検出可能。

```
元のレスポンス → SHA-256 → "e1c8d3f5a902b..."（監査ログに記録）

後日の検証:
保存データ → SHA-256 → "e1c8d3f5a902b..."（一致 → 改ざんなし）
                     → "7a2b1c..."       （不一致 → 改ざん検出!）
```

### 監査の3原則

1. **追記のみ（Append-only）**: 監査エントリは修正・削除できない
2. **非オプション**: 監査記録の失敗 = リクエスト全体の失敗
3. **同期的**: レスポンスを返す前に監査記録を完了

---

## 11. ユースケース1：配電系統の混雑管理

### シナリオ

夏の午後。Feeder F-101 (定格 5MW) にEV充電とエアコン負荷が集中し、熱容量の85%に達した。

### フロー

```
ACT 1: 参加者登録
  DSO (東京配電) → データスペースに参加
  Aggregator (GreenFlex) → データスペースに参加
  Prosumer (柏キャンパス) → データスペースに参加

ACT 2: 混雑検出
  DSO → 1000軒の負荷を監視 → 4236kW / 5000kW (85%) → アラート!

ACT 3: セキュリティ（不正アクセス試行）
  Spy → DSOデータにアクセス → 契約なし → 拒否（監査記録）
  Spy → 系統トポロジにアクセス → 感度ティア違反 → 拒否（監査記録）

ACT 4: 契約交渉
  Aggregator → カタログで制約データを発見
  Aggregator → DSO に契約を提案 → DSO が承認 → ACTIVE

ACT 5: 柔軟性提出
  Aggregator → 966kW の柔軟性エンベロープを提出
    EV充電器: 434kW (45%)
    制御可能負荷: 241kW (25%)
    蓄電池: 193kW (20%)
    ヒートポンプ: 97kW (10%)

ACT 6: DRディスパッチ
  DSO → Kafka で 236kW 削減指令 → Aggregator が実行
  結果: 4236kW → 4000kW (80%) → 混雑解消!

ACT 7: プライバシー
  Prosumer → research同意 → 統計値のみ返却
  Prosumer → 同意撤回 → 即座にアクセス拒否

ACT 8: 緊急オーバーライド
  DSO → 系統緊急事態 → 通常契約外でもアクセス可能（特別監査付き）

ACT 9: 監査証跡
  8件の監査エントリ: 5件成功、3件拒否（全てSHA-256ハッシュ付き）
```

### デモの実行

```bash
.venv/bin/python examples/congestion_management_demo.py
```

5枚の可視化が `examples/output/` に生成される。

---

## 12. ユースケース2：系統トポロジと電圧制御

### シナリオ

柏変電所（66/6.6kV, 10MVA）から2本のフィーダーが出ている。各フィーダーに125本の電柱、各電柱に4軒の住宅が接続。合計1000軒。

### 系統構成

```
柏変電所 (66/6.6kV)
  │
  ├── Feeder F-101 (2500kW定格)
  │     ├── P-001 ── [H-001, H-002, H-003, H-004]  (変電所に近い)
  │     ├── P-002 ── [H-005, H-006, H-007, H-008]
  │     ├── ...
  │     └── P-125 ── [H-497, H-498, H-499, H-500]  (末端、電圧低下大)
  │
  └── Feeder F-102 (2500kW定格)
        ├── P-126 ── [H-501, H-502, H-503, H-504]
        ├── ...
        └── P-250 ── [H-997, H-998, H-999, H-1000]
```

### 潮流計算（DistFlow 近似）

各電柱の電圧を、変電所からの距離と下流負荷から計算する：

```
V_drop ≈ (P × R + Q × X) / V²

P: 下流の有効電力 (kW)
Q: 下流の無効電力 (kvar)
R: 区間の抵抗 (Ω)
X: 区間のリアクタンス (Ω)
V: 現在の電圧 (V)
```

フィーダー末端ほど電圧降下が大きい。EV充電とエアコンの集中で、末端の電柱は 0.95pu を下回る（電圧逸脱）。

### DR による電圧回復

```
Phase 1: 潮流計算（DR前）
  F-101: 最低電圧 0.8987pu → 80柱で違反
  F-102: 最低電圧 0.8949pu → 82柱で違反
  合計: 162柱で電圧違反

Phase 2: データスペース経由でDR制御
  DSO → 電圧制約をカタログに公開
  Aggregator → 契約交渉 → ACTIVE
  Aggregator → 末端電柱のEV/蓄電池/ACを制御

Phase 3: 潮流計算（DR後）
  総負荷削減: 1043 kW
  違反柱: 162 → 119（26%改善）

データ主権:
  DSO: 柱ごとの集約負荷のみ見える。個別住宅は見えない
  Aggregator: 柔軟性エンベロープのみ見える。系統トポロジは見えない
  住宅: 自分のデータのみ。隣の家は見えない
```

### デモの実行

```bash
.venv/bin/python examples/grid_topology_demo.py
```

4枚の可視化が `examples/output/` に生成される。

---

## 13. セキュリティモデル詳解

### 脅威と対策のマッピング

| 脅威 | 対策 | 実装 |
|------|------|------|
| なりすまし | mTLS + OIDC 二重認証 | `auth.py` |
| 不正アクセス | 契約ベースのアクセス制御 | `policy.py`, `contract.py` |
| データ窃取 | 感度ティアによる階層的アクセス制御 | `PolicyEngine.evaluate()` |
| 目的外利用 | 契約の目的制限 + ポリシーチェック | `ContractOffer.purpose` |
| プライバシー侵害 | 目的別匿名化 + k-匿名化 | `anonymizer.py` |
| 同意の不正利用 | 即時撤回可能な同意管理 | `consent.py` |
| 改ざん | SHA-256 ハッシュによる改ざん検出 | `audit.py` |
| 監査逃れ | 監査失敗 = リクエスト失敗 | `ConnectorMiddleware` |
| トークン窃取 | 短い有効期限 + ローカルJWK検証 | `KeycloakAuthBackend` |

### 緊急オーバーライド

系統の安全を守るため、DSO には **緊急時の優先アクセス** が認められている：

```python
# 条件:
# 1. 要求者のロールが dso_operator
# 2. 契約に emergency_override=True フラグ
# 3. evaluate() に emergency=True を指定

decision = policy_engine.evaluate(
    requester_id="dso-001",
    asset_id="asset-flex-agg",
    contract=emergency_contract,  # emergency_override=True
    purpose="grid_emergency",
    operation="read",
    emergency=True,               # 緊急フラグ
)
# → allowed=True, emergency_override=True
# → 監査ログに「緊急アクセス」として特別記録
```

---

## 14. 技術スタック詳解

| 技術 | バージョン | 用途 |
|------|-----------|------|
| Python | 3.11+ | 全コンポーネントの実装言語 |
| FastAPI | 0.115+ | REST API フレームワーク |
| Uvicorn | 0.30+ | ASGI サーバー（SSL/TLS対応） |
| Pydantic | v2 | データモデル定義・バリデーション |
| Keycloak | 26.x | OIDC 認証基盤（Docker） |
| python-jose | 3.3+ | JWT トークン検証（ローカル JWK） |
| Apache Kafka | 3.8 | 非同期イベントバス（KRaft モード、Zookeeper 不要） |
| kafka-python | 2.0+ | Kafka クライアントライブラリ |
| SQLAlchemy | 2.0+ | ORM / データベース抽象化 |
| SQLite | — | 開発用ローカルストア |
| cryptography | 42.0+ | 証明書生成、暗号化操作 |
| httpx | 0.27+ | 非同期 HTTP クライアント |
| Docker Compose | — | マルチコンテナオーケストレーション |
| pytest | 8.0+ | テストフレームワーク（322テスト） |
| ruff | 0.8+ | リンター / フォーマッター |
| mypy | 1.11+ | 静的型チェッカー |

### なぜ Kafka か？

NATS JetStream も候補だったが、以下の理由で Kafka を採用：

- エネルギー業界での導入実績が豊富
- メッセージの永続化保証が強い
- パーティションによるスケーラビリティ
- KRaft モードで Zookeeper が不要に（運用簡素化）

---

## 15. デモの実行方法

### 環境構築

```bash
# リポジトリのクローン
git clone https://github.com/lutelute/data-space-grid.git
cd data-space-grid

# 依存関係のインストール
make setup
source .venv/bin/activate

# （オプション）matplotlib と numpy のインストール（デモ用）
pip install matplotlib numpy Pillow
```

### デモの実行

```bash
# 1000軒 混雑管理デモ（9枚の可視化出力）
python examples/congestion_management_demo.py

# 系統トポロジ + 潮流計算デモ（4枚の可視化出力）
python examples/grid_topology_demo.py
```

### テストの実行

```bash
make test              # 全322テスト
make test-unit         # ユニットテスト 226件
make test-integration  # インテグレーションテスト 96件
```

### Docker Compose での起動

```bash
# インフラ起動（Keycloak + Kafka）
docker compose up -d keycloak kafka

# 証明書生成
make certs

# 全サービス起動
make run-all
```

---

## 16. 用語集

| 用語 | 説明 |
|------|------|
| **DSO** | Distribution System Operator. 配電系統運用者。フィーダー制約・混雑信号を管理 |
| **Aggregator** | DER アグリゲーター。複数の分散電源をまとめて柔軟性を提供 |
| **Prosumer** | Producer + Consumer. 太陽光発電等を持つ需要家 |
| **Feeder** | フィーダー。変電所から需要家に電力を供給する配電線路 |
| **DER** | Distributed Energy Resources. EV、蓄電池、太陽光、ヒートポンプ等 |
| **DR** | Demand Response. 需要家側の負荷を制御して系統バランスを調整 |
| **Flexibility Envelope** | 柔軟性エンベロープ。DERフリートが提供可能な電力調整範囲 |
| **CIM** | Common Information Model. 電力系統のデータモデル国際標準 |
| **IEC 61850** | 変電所自動化・DER通信の国際標準 |
| **OpenADR** | Open Automated Demand Response. DRの自動化標準 |
| **mTLS** | mutual TLS. クライアント・サーバー双方が証明書で認証 |
| **OIDC** | OpenID Connect. OAuth 2.0 ベースの認証プロトコル |
| **JWT** | JSON Web Token. 認証情報を含む署名付きトークン |
| **JWK** | JSON Web Key. JWT の署名検証に使う公開鍵 |
| **k-匿名化** | データセット内の各レコードが少なくとも k-1 個の他レコードと区別不能であることの保証 |
| **感度ティア** | HIGH / MEDIUM / HIGH_PRIVACY の3段階データ分類 |
| **Data Space Connector** | データ交換の全てに認証・ポリシー・監査を自動適用するミドルウェア |
| **DistFlow** | 放射状配電系統の簡易潮流計算手法 |
| **pu (per-unit)** | 基準値に対する比率。電圧の場合、1.0pu = 定格電圧 |

---

*本解説書は Data Space Grid v0.1.0 に基づく。*
