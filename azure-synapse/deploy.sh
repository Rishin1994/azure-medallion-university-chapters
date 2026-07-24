#!/usr/bin/env bash
# =============================================================================
# Deploy the university_chapters medallion data product to Azure Synapse Spark.
#
# Creates: resource group, ADLS Gen2 account (+ 'synapse' and 'lake'
# filesystems), Synapse workspace, small auto-pausing Spark pool (runtime 3.5),
# role assignments, the medallion notebook, the daily pipeline and its
# 06:00 UTC trigger — then kicks off one run.
#
# Prerequisites:
#   * Azure CLI >= 2.60 logged in (az login) with Owner/Contributor on the sub
#   * Env vars:
#       export STG=<globally-unique storage name, e.g. stunichap<yourname>01>
#       export WS=<globally-unique workspace name, e.g. syn-unichap-<yourname>>
#       export SQL_ADMIN_PASSWORD=<strong password>   # never committed to git
#   * Run from the azure-synapse/ folder of the repo.
#
# Cost notes: the Spark pool bills vCore-hours ONLY while running and
# auto-pauses after 15 idle minutes; a daily few-minute run costs pennies.
# Tear everything down with:  az group delete --name "$RG" --yes
# =============================================================================
set -euo pipefail

: "${STG:?Set STG to a globally-unique lowercase storage account name}"
: "${WS:?Set WS to a globally-unique Synapse workspace name}"
: "${SQL_ADMIN_PASSWORD:?Set SQL_ADMIN_PASSWORD (workspace SQL admin; not used by this pipeline)}"
RG="${RG:-rg-university-chapters}"
LOC="${LOC:-eastus2}"
POOL="${POOL:-sparkmed}"          # 1-15 chars, letters+digits only
LAKE_FS="lake"

step() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

step "1/9 Resource group"
az group create --name "$RG" --location "$LOC" --output none

step "2/9 ADLS Gen2 storage account + filesystems"
az storage account create --name "$STG" --resource-group "$RG" --location "$LOC" \
  --sku Standard_LRS --kind StorageV2 --hns true --min-tls-version TLS1_2 --output none
az storage fs create --name synapse --account-name "$STG" --auth-mode login --output none || true
az storage fs create --name "$LAKE_FS" --account-name "$STG" --auth-mode login --output none || true

step "3/9 Synapse workspace (this takes a few minutes)"
az synapse workspace create --name "$WS" --resource-group "$RG" --location "$LOC" \
  --storage-account "$STG" --file-system synapse \
  --sql-admin-login-user sqladminuser --sql-admin-login-password "$SQL_ADMIN_PASSWORD" \
  --output none
MYIP=$(curl -s https://ifconfig.me)
az synapse workspace firewall-rule create --name allow-my-ip --workspace-name "$WS" \
  --resource-group "$RG" --start-ip-address "$MYIP" --end-ip-address "$MYIP" --output none

step "4/9 Storage roles (workspace MSI + you)"
STG_ID=$(az storage account show --name "$STG" --resource-group "$RG" --query id -o tsv)
WS_MSI=$(az synapse workspace show --name "$WS" --resource-group "$RG" \
  --query identity.principalId -o tsv)
ME=$(az ad signed-in-user show --query id -o tsv)
az role assignment create --role "Storage Blob Data Contributor" \
  --assignee-object-id "$WS_MSI" --assignee-principal-type ServicePrincipal \
  --scope "$STG_ID" --output none || true
az role assignment create --role "Storage Blob Data Contributor" \
  --assignee-object-id "$ME" --assignee-principal-type User \
  --scope "$STG_ID" --output none || true

step "5/9 Spark pool (runtime 3.5 = Spark 3.5 / Delta 3.2; Small nodes; auto-pause 15 min)"
az synapse spark pool create --name "$POOL" --workspace-name "$WS" --resource-group "$RG" \
  --spark-version 3.5 --node-count 3 --node-size Small \
  --enable-auto-pause true --delay 15 --output none

step "6/9 Upload the DQ fixture to the lake (enables source=fixture runs in the cloud)"
az storage fs file upload --account-name "$STG" --file-system "$LAKE_FS" \
  --source ../fixtures/university_chapters_fixture.json \
  --path fixtures/university_chapters_fixture.json --auth-mode login --overwrite true --output none

step "7/9 Import the medallion notebook"
az synapse notebook import --workspace-name "$WS" --name university_chapters_medallion \
  --file @notebook/university_chapters_medallion.ipynb --spark-pool-name "$POOL" --output none

step "8/9 Pipeline + daily 06:00 UTC trigger"
render() { # substitute placeholders and strip to the 'properties' body az expects
  python3 - "$1" <<'PY'
import json, sys, os
doc = json.load(open(sys.argv[1]))
text = json.dumps(doc["properties"])
text = text.replace("__STORAGE_ACCOUNT__", os.environ["STG"])
text = text.replace("__SPARK_POOL__", os.environ["POOL"])
text = text.replace("__START_TIME__", os.environ.get("TRIGGER_START", ""))
print(text)
PY
}
export STG POOL
export TRIGGER_START=$(python3 -c "from datetime import datetime,timedelta,timezone; \
print((datetime.now(timezone.utc)+timedelta(days=1)).strftime('%Y-%m-%dT06:00:00Z'))")
render pipeline/pl_university_chapters_daily.json > /tmp/pl_rendered.json
az synapse pipeline create --workspace-name "$WS" \
  --name pl_university_chapters_daily --file @/tmp/pl_rendered.json --output none
render trigger/tr_daily_0600_utc.json > /tmp/tr_rendered.json
az synapse trigger create --workspace-name "$WS" \
  --name tr_daily_0600_utc --file @/tmp/tr_rendered.json --output none
az synapse trigger start --workspace-name "$WS" --name tr_daily_0600_utc --output none

step "9/9 Kick off one live run now"
RUN_ID=$(az synapse pipeline create-run --workspace-name "$WS" \
  --name pl_university_chapters_daily \
  --parameters "{\"storage_account\":\"$STG\",\"lake_container\":\"$LAKE_FS\",\"source\":\"live\"}" \
  --query runId -o tsv)
echo "Pipeline run started: $RUN_ID"
echo "Watch it:   az synapse pipeline-run show --workspace-name $WS --run-id $RUN_ID"
echo "Studio:     https://web.azuresynapse.net (workspace: $WS)  ->  Monitor -> Pipeline runs"
echo
echo "Done. Gold will land at abfss://$LAKE_FS@$STG.dfs.core.windows.net/gold/university_chapters/v1"
