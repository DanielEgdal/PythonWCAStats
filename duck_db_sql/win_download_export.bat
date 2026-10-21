@echo off
setlocal

if not exist "WCA_export" mkdir "WCA_export"

del /Q "WCA_export\*"

curl -L "https://www.worldcubeassociation.org/export/results/v2/tsv" -o "WCA_export\WCA_export.tsv.zip"

powershell -Command "Expand-Archive -Path 'WCA_export\WCA_export.tsv.zip' -DestinationPath 'WCA_export' -Force"

echo Done!