# StreamDuck-BI

## 概要

Streamlit+DuckDB を用いた BI ツール

## 環境構築

### 開発環境

1. リポジトリのルートディレクトリに`.env`を作成し、以下を設定

```
# 共通
S3_BUCKET=warehouse（MINIOにてデフォルトで作成されているバケット）

# MINIO（ローカル）環境設定
MINIO_ACCESS_KEY_ID=admin
MINIO_SECRET_ACCESS_KEY=password
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
MINIO_DOMAIN=minio
TZ=Asia/Tokyo

# S3環境用設定(適宜変更)
AWS_ACCESS_KEY_ID=admin（仮）
AWS_SECRET_ACCESS_KEY=password（仮）
AWS_REGION=us-east-1（仮）
```

2. Dev Container を起動

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
