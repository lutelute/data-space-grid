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

## 17. デモで起きていることの詳細プロセス

本章では、デモの各ステップで **何が** 、**なぜ** 、**どのコードで** 起きているかを追跡する。

### 17.1 混雑管理デモ：1ステップずつ

#### Step 0: インフラ初期化

```
ContractManager()  → 契約の状態遷移マシンを起動
PolicyEngine()     → ポリシー判定エンジンを起動
AuditLogger()      → 監査ログファイルを準備（JSONL形式）
```

この時点ではデータ交換は一切不可能。参加者もアセットも登録されていない。

#### Step 1: 参加者登録 — 「この世界に誰がいるか」

```python
dso = Participant(
    id="dso-001",
    name="Tokyo Distribution Grid",
    roles=["dso_operator"],              # ← このロールが感度ティアの鍵
    certificate_dn="CN=dso-001,O=Tokyo DSO,C=JP",  # ← mTLS で検証される
)
policy_engine.register_participant(dso)
```

**ここで何が起きるか:**
- PolicyEngine の内部テーブルに `dso-001` が登録される
- 以後のポリシー評価で `dso-001` のロール（`dso_operator`）が参照される
- `dso_operator` ロールは `HIGH` 感度のデータにアクセス可能
- `aggregator` ロールは `MEDIUM` まで
- `unknown` ロールはどの感度ティアにもアクセス不可

**なぜ全員を登録するのか:**
未登録の参加者からのリクエストは `ParticipantNotRegisteredError` で自動拒否される。これにより「知らない相手とはデータを交換しない」が強制される。

#### Step 2: 負荷シミュレーション — 「現実の電力系統をモデル化」

```python
data = generate_households(1000)
# → 1000軒の15分間隔負荷プロファイルを生成
#    EV充電 (300軒): 3-7kW、16:30頃にピーク
#    AC (700軒): 1-2.5kW、12:00-18:00
#    ヒートポンプ (100軒): 1.5-3kW
#    蓄電池 (200軒): 5-13kWh
```

**ここで何が起きるか:**
- NumPy で1000軒 × 96タイムステップ（24時間/15分）の負荷行列を生成
- 各世帯にランダムにDER機器を割り当て
- ベース負荷 + EV + AC + HP を合計 → フィーダー全体で 4236kW
- 制御可能負荷（DR対象）を別途計算

**なぜ15分間隔か:**
日本の電力市場のインバランス計量が30分単位。15分間隔はその半分で、需要予測にも十分な粒度。

#### Step 3: 混雑検出と制約公開 — 「問題の定義」

```python
constraint = FeederConstraint(
    feeder_id="F-101",
    max_active_power_kw=5000,
    congestion_level=0.85,    # ← 4236/5000 = 84.7%
)
```

```python
constraint_asset = DataAsset(
    id="asset-constraint-f101",
    provider_id="dso-001",
    sensitivity=SensitivityTier.MEDIUM,  # ← アグリゲーターがアクセス可能
    policy_metadata={"purpose": "congestion_management", "contract_required": "true"},
)
policy_engine.register_asset(constraint_asset)
```

**ここで何が起きるか:**
1. DSO が CIM モデルでフィーダー制約を作成
2. そのメタデータ（実データではない）をカタログに登録
3. `policy_metadata` に「混雑管理目的」「契約必須」を明記
4. `MEDIUM` 感度なので `aggregator` ロールからアクセス可能（契約があれば）

#### Step 4: 不正アクセスの拒否 — 「セキュリティの証明」

```python
# Spy は OFFERED 状態の契約しか持っていない
fake_contract = DataUsageContract(status=ContractStatus.OFFERED, ...)

decision = policy_engine.evaluate(
    requester_id="spy-001",
    contract=fake_contract,
    purpose="espionage",
)
# → allowed=False
# → reason="contract is not active (status=offered)"
```

**ここで何が起きるか:**
1. PolicyEngine が契約のステータスを確認 → `OFFERED` は `ACTIVE` ではない → 拒否
2. 仮に `ACTIVE` だったとしても、目的が `"espionage"` なので契約の目的と不一致 → 拒否
3. さらに `spy-001` のロールが `"unknown"` なので感度ティアチェックでも拒否
4. **3重の防御**が機能している

**監査記録:**
```
audit_log.log_exchange(
    requester_id="spy-001",
    outcome=AuditOutcome.DENIED,  # ← 拒否も記録される
)
```

拒否されたアクセスも全て記録される。これにより不正アクセスの試行を事後的に検知・調査できる。

#### Step 5: 契約交渉 — 「信頼関係の構築」

```python
offer = ContractOffer(
    provider_id="dso-001",
    consumer_id="agg-001",
    purpose="congestion_management",
    emergency_override=True,       # ← 緊急時のDSOアクセスを許可
    valid_from=NOW,
    valid_until=NOW + 90日,
)

contract = contract_mgr.offer_contract(offer)    # → status: OFFERED
contract_mgr.negotiate_contract(contract_id)     # → status: NEGOTIATING
contract = contract_mgr.accept_contract(contract_id)  # → status: ACTIVE
```

**ここで何が起きるか:**
1. `offer_contract`: 新しい契約IDを生成し、`OFFERED` 状態で保存
2. `negotiate_contract`: 状態を `NEGOTIATING` に遷移（条件修正フェーズ）
3. `accept_contract`: 状態を `ACTIVE` に遷移 → データ交換が可能に

**なぜ3ステップか:**
現実のビジネスでは、最初の条件提示がそのまま受諾されることは少ない。`NEGOTIATING` フェーズで条件を摺り合わせる余地を設計に組み込んでいる。

#### Step 6: 柔軟性エンベロープ — 「個別DERを隠す」

```python
envelope = FlexibilityEnvelope(
    pq_range=PQRange(p_min_kw=-966, p_max_kw=0, ...),
    device_class_mix=[
        DeviceClassMix(der_type=DERType.EV_CHARGER, share_pct=45, ...),
        DeviceClassMix(der_type=DERType.BATTERY_STORAGE, share_pct=20, ...),
    ],
    response_confidence=ResponseConfidence(level=ConfidenceLevel.HIGH, probability_pct=92),
)
```

**ここで何が起きるか:**
- 1000台のDERの状態を**集約**して1つのエンベロープにする
- DSO に見えるのは「966kW の下げ柔軟性、EV 45%・蓄電池 20%」だけ
- 個別のEVの充電状態、個別の蓄電池のSOCは**一切見えない**

**なぜこれが重要か:**
個別DERの状態はアグリゲーターのビジネス資産。DSO に渡す必要はない。DSO が必要なのは「このフィーダーで何kW下げられるか」という集約情報だけ。

#### Step 7: ディスパッチと実績 — 「制御の実行」

```python
dispatch_cmd = DispatchCommand(
    target_power_kw=236,           # 236kW 削減要求
    activation_time=NOW + 15min,
    duration_minutes=300,
)
# → Kafka topic "dispatch-commands" 経由で送信

actual = DispatchActual(
    commanded_kw=236,
    delivered_kw=222,              # 94% の実績
    delivery_accuracy_pct=94.0,
)
# → Kafka topic "dispatch-actuals" 経由で報告
```

**なぜ Kafka か:**
ディスパッチ指令は**リアルタイム性**が要求される。REST API のリクエスト-レスポンスでは遅延が発生し、特にネットワーク障害時に指令が失われる可能性がある。Kafka はメッセージを永続化し、受信者がオフラインでも回復後に指令を受け取れる。

#### Step 8: プライバシーパイプライン — 「目的が出力を決める」

```python
# 同じデータに対して、目的によって異なる出力
result_research = anonymizer.anonymize_demand_profile(profile, "research")
# → AnonymizedLoadSeries (統計値のみ、ID除去)

result_dr = anonymizer.anonymize_demand_profile(profile, "dr_dispatch")
# → ControllableMarginResult (75.9 kW というスカラー値のみ)
```

**ここで何が起きるか:**
1. `anonymize_demand_profile` が目的を `PURPOSE_DISCLOSURE_MAP` で参照
2. `"research"` → `AGGREGATED` → 平均・標準偏差・ピークのみ、ID除去
3. `"dr_dispatch"` → `CONTROLLABILITY_ONLY` → 制御可能マージンの単一スカラー値

**同意撤回の即時性:**
```python
consent_mgr.revoke_consent(consent_id)
# → この時点で、次のリクエストは即座に拒否される
# → 「そのうち反映」ではなく「今この瞬間から」拒否
```

---

## 18. なぜデータスペースが必要か

### 18.1 現状の問題：データ共有の信頼危機

電力セクターに限らず、組織間のデータ共有は以下の「信頼の壁」に阻まれている：

#### 問題1: 「渡したら終わり」問題

```
従来のAPI連携:

  DSO ──── API ────> Aggregator
         （データ転送）

  転送後、DSO はデータの行方を制御できない。
  Aggregator が:
    ✗ データを第三者に売却するかもしれない
    ✗ 本来の目的と異なる用途に使うかもしれない
    ✗ 必要以上に長期間保持するかもしれない
    ✗ 適切なセキュリティで管理しないかもしれない

データスペースの解決策:

  DSO ──── Connector ────> Aggregator
         │                │
         │ 契約で制限:     │
         │ • 目的: 混雑管理のみ
         │ • 保持: 30日    │
         │ • 再配布: 禁止  │
         │ • 監査: 全記録  │
```

#### 問題2: プライバシーの構造的保護の欠如

```
従来のアプローチ:
  「個人情報保護方針に同意します ☑」→ 全データが相手に渡る
  → 形式的同意であり、技術的保護がない

データスペースのアプローチ:
  目的「研究」→ 統計値のみ自動変換して渡す
  目的「DR」  → 制御マージンのスカラー値のみ渡す
  目的なし    → 何も渡さない
  → 技術的に不可能な状態を作る（生データが物理的に外に出ない）
```

#### 問題3: 監査不能な「闇のデータフロー」

```
従来のシステム:
  「誰がいつどのデータにアクセスしたか？」
  → ログがあるかもしれないが、改ざん可能
  → 事業者間で形式が異なり、突合できない

データスペースの解決策:
  全交換を SHA-256 ハッシュ付きで記録
  → リクエストとレスポンスの内容を暗号的に固定
  → 後日「このデータは改ざんされていない」を数学的に証明可能
```

### 18.2 データスペースがもたらす価値

| 価値 | 従来 | データスペース |
|------|------|---------------|
| **データ主権** | データを渡したら制御不能 | 契約で利用条件を機械的に強制 |
| **信頼構築コスト** | 個別にNDA/データ利用契約 → 数ヶ月 | 標準化されたコネクタで自動契約 → 数分 |
| **プライバシー** | 形式的同意 | 技術的に生データの流出を防止 |
| **監査** | 各社バラバラのログ | 統一フォーマットで改ざん検出可能 |
| **相互運用性** | 個別API開発 | セマンティックモデルで標準化 |
| **スケーラビリティ** | N社間の連携 = N×(N-1)/2 の個別契約 | N社がデータスペースに参加するだけ |

### 18.3 なぜ今、データスペースが必要なのか

#### エネルギー転換の加速

```
2020年: EV 100万台 → 2030年: EV 3000万台（日本政府目標）

EV が増えると:
  → 夕方の充電集中 → フィーダーの熱容量超過
  → 解決策: スマート充電（アグリゲーターが充電タイミングを制御）
  → しかし: DSO のフィーダー制約データが必要
  → しかし: DSO は系統トポロジをアグリゲーターに渡したくない
  → 解決策: データスペースで必要最小限の制約データのみ、契約付きで共有
```

#### 規制の動向

- **欧州**: Data Act (2024)、GDPR、再エネ指令で「データ主権」「目的制限」が法的要件に
- **日本**: 電力データ活用の議論が進行中。スマートメーターデータの第三者提供ルール整備中
- **国際**: IEC 63417 (Data Space for Energy) の標準化が進行中

#### ビジネスの現実

```
DSO の本音:
  「データを共有したい。でも系統トポロジは企業秘密。」
  「共有するなら、相手の使い方を制御したい。」

Aggregator の本音:
  「DSO の制約データがないと最適なDR制御ができない。」
  「でも個別DERの状態は自社の競争優位。渡したくない。」

Prosumer の本音:
  「電気代が下がるなら協力する。でもプライバシーは守りたい。」
  「いつでも参加をやめられるようにしたい。」

→ 全員の要求を同時に満たすのがデータスペース
```

---

## 19. 他分野への展開可能性

本プロジェクトのアーキテクチャは電力セクターに特化しているが、**コネクタの仕組み自体は汎用的**であり、同様の「データ主権を保ちつつ組織間でデータを共有したい」課題を持つ分野に応用可能である。

### 19.1 横展開が可能な分野

#### 医療・ヘルスケア

```
現在の課題:
  病院A の患者データを研究機関B が使いたい
  → 個人情報保護法の壁
  → 患者の同意管理が煩雑
  → データの二次利用追跡が困難

データスペースで解決:
  参加者: 病院 / 研究機関 / 患者
  契約: 「匿名化した画像データを、がん研究目的でのみ、2年間利用可能」
  匿名化: 目的=研究 → k-匿名化 + 準識別子除去
  監査: 誰がいつどの患者データにアクセスしたか完全記録
  同意: 患者がスマホから即座に同意撤回可能
```

| 電力セクター | 医療セクター |
|-------------|-------------|
| DSO | 病院 |
| Aggregator | 研究機関・製薬会社 |
| Prosumer | 患者 |
| FeederConstraint | 診療データ |
| FlexibilityEnvelope | 匿名化臨床データ |
| DemandProfile | 患者バイタルデータ |
| ConsentRecord | 患者同意記録 |

#### 製造業・サプライチェーン

```
現在の課題:
  自動車メーカーが部品サプライヤーの品質データを確認したい
  → サプライヤーは製造プロセスの詳細を開示したくない
  → 品質証明だけ共有したい

データスペースで解決:
  参加者: OEM / Tier1サプライヤー / Tier2サプライヤー
  データ主権: 各サプライヤーの製造データはローカルに保持
  契約: 「品質証明書データを、製品トレーサビリティ目的でのみ共有」
  匿名化: 合格/不合格の集計のみ。個別製造パラメータは非開示
```

| 電力セクター | 製造業 |
|-------------|--------|
| DSO | OEM（自動車メーカー） |
| Aggregator | Tier1サプライヤー |
| Prosumer | Tier2サプライヤー |
| CongestionSignal | 品質アラート |
| FlexibilityEnvelope | 生産能力エンベロープ |
| 監査証跡 | トレーサビリティ記録 |

#### モビリティ・MaaS

```
現在の課題:
  鉄道会社、バス会社、タクシー、シェアサイクルのデータを統合したい
  → 各社の乗客データは機密
  → しかし需要予測には横断データが必要

データスペースで解決:
  参加者: 鉄道 / バス / タクシー / 自治体
  共有データ: 匿名化された乗降統計（個人の移動履歴ではなく）
  契約: 「混雑緩和目的でのみ」「30日保持」
  感度ティア: 個人移動データ = HIGH_PRIVACY, 駅別統計 = MEDIUM
```

#### 不動産・スマートシティ

```
参加者: ビルオーナー / エネルギー会社 / 自治体
共有データ: ビルのエネルギー消費（匿名化）
目的: 地域のカーボンフットプリント計算
契約: 「CO2算定目的でのみ」「年次集計のみ」
```

#### 農業・食品トレーサビリティ

```
参加者: 農家 / 流通 / 小売 / 消費者
共有データ: 生産履歴、農薬使用記録、温度管理記録
目的: 食の安全、トレーサビリティ
契約: 「食品安全確認目的」「産地偽装の検出」
```

### 19.2 再利用可能なコンポーネント

本プロジェクトのコードで、電力に依存しない部分は以下の通り：

| コンポーネント | 場所 | 汎用性 |
|---------------|------|--------|
| **ContractManager** | `src/connector/contract.py` | 完全に汎用。どの分野でもそのまま使用可能 |
| **PolicyEngine** | `src/connector/policy.py` | 感度ティアとロールの定義を変えるだけ |
| **AuditLogger** | `src/connector/audit.py` | 完全に汎用。SHA-256ハッシュ + JSONL |
| **ConnectorMiddleware** | `src/connector/middleware.py` | FastAPI アプリならそのまま使用可能 |
| **ConsentManager** | `src/participants/prosumer/consent.py` | 目的リストを変えるだけ |
| **DataAnonymizer** | `src/participants/prosumer/anonymizer.py` | 開示レベルマッピングを変えるだけ |
| **KeycloakAuthBackend** | `src/connector/auth.py` | Keycloak を使う限りそのまま |

**変更が必要な部分:**

| コンポーネント | 変更内容 |
|---------------|---------|
| `src/semantic/` | 分野固有のデータモデルに差し替え（FHIR、GS1等） |
| `src/participants/` | 参加者ノードを分野に合わせて実装 |
| `PURPOSE_DISCLOSURE_MAP` | 分野の目的と開示レベルのマッピングを再定義 |
| `_DEFAULT_TIER_ROLES` | 分野のロールと感度ティアの対応を再定義 |

### 19.3 導入のステップ

他分野でデータスペースを構築する場合の推奨ステップ：

```
Step 1: ステークホルダー分析
  → 誰が参加するか？各者のデータ主権の要求は？
  → 例: 病院は患者データの主権を持つ。研究機関は解析結果の主権を持つ。

Step 2: データアセットの洗い出し
  → どんなデータが存在するか？感度分類は？
  → 例: 患者バイタル=HIGH_PRIVACY, 匿名化統計=MEDIUM

Step 3: ユースケースの定義
  → どんなデータ交換が必要か？目的は？
  → 例: がん研究のために匿名化画像を共有

Step 4: セマンティックモデルの定義
  → 業界標準があればそれを採用（FHIR, GS1, IFC等）
  → なければ Pydantic で定義

Step 5: 目的-開示レベルマッピングの定義
  → 各目的に対して何を開示するか

Step 6: コネクタの設定
  → 本プロジェクトの connector/ をそのまま使用
  → Keycloak レルムを設定
  → 参加者ノードを実装

Step 7: パイロット運用
  → 2-3参加者で小規模に開始
  → 契約テンプレートを整備

Step 8: スケール
  → 参加者を追加（コネクタ方式なのでN対Nが容易）
```

### 19.4 国際的な動向

| イニシアチブ | 分野 | 関連性 |
|-------------|------|--------|
| **GAIA-X** | 欧州横断クラウド | 本プロジェクトの設計思想の源泉 |
| **Catena-X** | 自動車産業 | サプライチェーンのデータスペース（VW, BMW等が参加） |
| **Manufacturing-X** | 製造業全般 | Catena-X を他製造業に拡張 |
| **Mobility Data Space** | モビリティ | ドイツの交通データスペース |
| **European Health Data Space** | 医療 | EU全域の医療データ共有基盤 |
| **Smart Energy Data Space** | エネルギー | 本プロジェクトが参考にしたドメイン |
| **IDS Reference Architecture** | 汎用 | データスペースのリファレンスアーキテクチャ |
| **Eclipse Dataspace Connector** | 汎用 | IDSA のオープンソース実装（Java） |

本プロジェクト（Data Space Grid）は、これらの大規模イニシアチブの **設計原則を Python で軽量に実装した研究プロトタイプ** という位置づけである。本番環境では Eclipse Dataspace Connector (EDC) 等の成熟した実装と組み合わせることが想定される。

---

## 20. 標準化への道筋

データスペースを一企業の独自ソリューションではなく、業界全体のインフラにするためには標準化が不可欠である。本章では、何を・どこで・どのように標準化すべきかを議論する。

### 20.1 標準化すべき4つのレイヤー

```
┌─────────────────────────────────────────────────────────┐
│  Level 4: ビジネスルール                                  │
│    契約テンプレート、SLA、料金体系                          │
│    → 業界団体・規制当局が主導                              │
├─────────────────────────────────────────────────────────┤
│  Level 3: セマンティックモデル                             │
│    データの意味と構造の統一                                │
│    → 国際標準化機関 (IEC, ISO, IEEE)                      │
├─────────────────────────────────────────────────────────┤
│  Level 2: コネクタプロトコル                               │
│    契約交渉、ポリシー交換、監査フォーマット                  │
│    → IDSA, Eclipse Foundation                            │
├─────────────────────────────────────────────────────────┤
│  Level 1: 通信・認証基盤                                  │
│    mTLS, OIDC, Kafka プロトコル                           │
│    → IETF, OpenID Foundation, Apache                     │
└─────────────────────────────────────────────────────────┘
```

**Level 1 と 2 は既に標準がある。** 本プロジェクトの主な標準化貢献は Level 3 と 4 に関わる。

### 20.2 セマンティックモデルの標準化

#### 現状: 規格の乱立

```
系統データ:
  CIM (IEC 61970/61968) → 欧米の送配電
  IEC 61850 → 変電所自動化
  DLMS/COSEM → スマートメーター
  → 3つの規格が重なり合い、マッピングが必要

DER/DR:
  OpenADR 2.0b → デマンドレスポンス
  IEEE 2030.5 → スマートエネルギープロファイル
  IEC 61850-7-420 → DER情報モデル
  → 概念は似ているが、データ構造が異なる

需要家データ:
  規格なし → 各社独自
```

#### 本プロジェクトのアプローチ: Pydantic による「実行可能な仕様」

```python
# 従来の標準化: 100ページのPDF仕様書
#   → 解釈の余地が大きい
#   → 実装者によって異なる解釈
#   → テストが困難

# 本プロジェクト: Pydantic モデル = 実行可能な仕様
class FeederConstraint(BaseModel):
    feeder_id: str
    max_active_power_kw: float = Field(..., gt=0)  # ← バリデーション付き
    congestion_level: float = Field(..., ge=0, le=1)  # ← 範囲制約
    sensitivity: SensitivityTier = SensitivityTier.MEDIUM  # ← デフォルト値
```

**メリット:**
- **曖昧さがない**: コードがそのまま仕様。「最大有効電力は正の値」がバリデーションで強制される
- **テスト可能**: 226のユニットテストが仕様の正確性を保証
- **バージョン管理**: Git で仕様の変更履歴を追跡可能
- **自動ドキュメント**: FastAPI の OpenAPI/Swagger が自動生成される

#### 標準化への推奨ステップ

```
Phase 1: 実装ベースの合意形成（6-12ヶ月）
  → 3-5の事業者がプロトタイプを共同テスト
  → Pydantic モデルを共同で改善
  → 実装を通じて仕様の問題点を洗い出す

Phase 2: 業界プロファイルの策定（12-18ヶ月）
  → 共同テストの結果を基に「日本版エネルギーデータスペースプロファイル」を策定
  → CIM/IEC 61850 の既存標準との対応表を作成
  → JSON Schema として公開（Pydantic モデルから自動生成可能）

Phase 3: 国際標準への提案（18-36ヶ月）
  → IEC TC57 (電力系統通信) に提案
  → IEC 63417 (Data Space for Energy) への貢献
  → OpenADR Alliance との連携
```

### 20.3 契約テンプレートの標準化

契約の条件フィールドは標準化可能であり、そうすべきである：

```python
# 現在（本プロジェクト）: 自由記述
ContractOffer(
    purpose="congestion_management",        # ← 文字列
    allowed_operations=["read"],            # ← リスト
    retention_days=30,                      # ← 数値
)

# 将来（標準化後）: 列挙型 + コードリスト
ContractOffer(
    purpose=StandardPurpose.CONGESTION_MANAGEMENT,  # ← IEC定義の目的コード
    allowed_operations=[StandardOp.READ],            # ← 標準操作コード
    retention=RetentionPolicy.DAYS_30,               # ← 標準保持ポリシー
)
```

#### 契約テンプレートの例

```yaml
# congestion_management_template.yaml
template_id: "JPDS-CM-001"
name: "配電系統混雑管理契約テンプレート"
version: "1.0"

parties:
  provider:
    role: dso_operator
    obligations:
      - publish_constraints_within: 5min
      - update_frequency: 15min
  consumer:
    role: aggregator
    obligations:
      - respond_to_dispatch_within: 15min
      - report_actuals_within: 1hour

data_access:
  assets: [feeder_constraint, congestion_signal]
  operations: [read]
  sensitivity_max: medium

privacy:
  anonymization_required: false  # 系統データなので不要
  retention_max_days: 90
  redistribution: prohibited

emergency:
  dso_override: true
  notification: required
  audit: enhanced

sla:
  availability: 99.5%
  latency_max_ms: 500
  support_hours: "24/7"
```

### 20.4 相互運用性テストの標準化

異なるベンダーのコネクタが接続できることを保証するテスト仕様：

```
┌─────────────────────────────────────────────────────────┐
│  相互運用性テストスイート                                  │
│                                                         │
│  Test 1: 認証接続テスト                                   │
│    コネクタA → mTLS + OIDC → コネクタB                    │
│    期待: 正しい証明書で接続成功、不正証明書で接続拒否        │
│                                                         │
│  Test 2: カタログ相互運用テスト                            │
│    コネクタA がアセット登録 → コネクタB が検索・発見          │
│    期待: 検索結果にアセットが含まれる                       │
│                                                         │
│  Test 3: 契約交渉テスト                                   │
│    コネクタA が提案 → コネクタB が承認                      │
│    期待: 両者で契約ステータスが一致                         │
│                                                         │
│  Test 4: ポリシー強制テスト                                │
│    契約なしでデータ要求 → 拒否                             │
│    契約ありで要求 → 許可                                  │
│    期待: 全交換に監査エントリ                              │
│                                                         │
│  Test 5: 匿名化テスト                                    │
│    目的=research でデータ要求                              │
│    期待: 個人特定不可能な集計データのみ返却                  │
│                                                         │
│  Test 6: 監査整合性テスト                                  │
│    複数交換後、監査ログの完全性を検証                       │
│    期待: 全交換に対応するエントリ、ハッシュ一致              │
└─────────────────────────────────────────────────────────┘
```

**本プロジェクトの322テスト**は、この相互運用性テストスイートの原型として機能する。

### 20.5 日本における推進体制の提案

```
推進主体の候補:
  → 経済産業省 資源エネルギー庁（政策・規制面）
  → NEDO / JST（研究開発資金）
  → 電気学会 / 電力中央研究所（技術標準）
  → OCCTO（広域的運営推進機関、系統データの管理）
  → 各一般送配電事業者（実証フィールド）

段階的アプローチ:
  Year 1: 研究会の設立
    → 本プロジェクトのような研究プロトタイプを複数開発
    → ユースケースの洗い出しと優先順位付け

  Year 2: 実証事業
    → 2-3の送配電事業者エリアでパイロット
    → アグリゲーター2-3社、需要家100-1000軒規模
    → 契約テンプレートの実地検証

  Year 3: 業界ガイドライン策定
    → 実証の知見を基にガイドライン化
    → セマンティックモデルの業界合意
    → 相互運用性テスト仕様の確定

  Year 4-5: 国際標準への貢献
    → IEC TC57 への提案
    → GAIA-X / IDSA への日本プロファイル登録
    → アジア太平洋地域への展開
```

### 20.6 オープンソースと標準化の関係

```
オープンソース実装が標準化を加速する理由:

  1. 「動くコード」が最強の仕様書
     → 100ページの仕様書より、動くプロトタイプの方が合意形成が早い

  2. フォーク可能
     → 医療版、製造版、モビリティ版を同じ基盤から派生可能

  3. テスト駆動
     → 322テストが「仕様に準拠しているか」を自動検証

  4. コミュニティ
     → 利用者からのフィードバックが仕様を改善

本プロジェクトの位置づけ:
  → 標準化議論の「叩き台」としてのリファレンス実装
  → MITライセンスで商用利用も自由
  → セマンティックモデル部分を差し替えれば他分野にも適用可能
```

---

*本解説書は Data Space Grid v0.1.0 に基づく。*
