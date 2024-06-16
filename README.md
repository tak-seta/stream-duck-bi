# StreamDuck-BI

## 概要
Streamlit+DuckDBを用いたBIツール

## 環境構築
### 開発環境
1. リポジトリのルートディレクトリに`.env`を作成し、以下を設定
```
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_DOMAIN=minio
TZ=Asia/Tokyo
S3_BUCKET=warehouse（デフォルトで作成されているバケット）
```

2. Dev Containerを起動

[こちら](https://code.visualstudio.com/docs/devcontainers/containers)を参考

2. ライブラリのインストール
```
rye sync
```
## アプリケーション(Streamlit)の起動
### 開発環境
```
rye run streamlit run src/app.py
```
