#directory to save files
#outdir=/nird/datalake/NS9873K/etdu/raw/smile/spear_med_le/scandinavia/PRECT/
#GFDL large ensembles ftp site
#ftp_site=ftp://nomads.gfdl.noaa.gov/2/GFDL-LARGE-ENSEMBLES/CMIP/NOAA-GFDL/GFDL-SPEAR-MED
#enter ${outdir} directory
#cd ${outdir}
#download file with curl command
#curl -O ${ftp_site}/historical/r10i1p1f1/day/pr/gr3/v20210201/pr_day_GFDL-SPEAR-MED_historical_r10i1p1f1_gr3_19210101-19301231.nc

#!/usr/bin/env bash

set -euo pipefail

# =========================
# User settings
# =========================

outdir="/nird/datalake/NS9873K/etdu/raw/smile/spear_med_le/scandinavia/PRECT"
ftp_site="ftp://nomads.gfdl.noaa.gov/2/GFDL-LARGE-ENSEMBLES/CMIP/NOAA-GFDL/GFDL-SPEAR-MED"

# Variable info
var="pr"
freq="day"
grid="gr3"
version="v20210201"
model="GFDL-SPEAR-MED"

# Which simulation types to download
# Edit this list as needed
simulations=(
  "historical"
  "scenarioSSP5-85"
)

# Maximum number of files to download in one run
# Set to a large number if you want everything
MAX_FILES=20

# Skip files that already exist?
SKIP_EXISTING=true

# =========================
# Ensemble members
# =========================

members=(
  "r1i1p1f1"
  "r2i1p1f1"
  "r3i1p1f1"
  "r4i1p1f1"
  "r5i1p1f1"
  "r6i1p1f1"
  "r7i1p1f1"
  "r8i1p1f1"
  "r9i1p1f1"
  "r10i1p1f1"
  "r11i1p1f1"
  "r12i1p1f1"
  "r13i1p1f1"
  "r14i1p1f1"
  "r15i1p1f1"
  "r16i1p1f1"
  "r17i1p1f1"
  "r18i1p1f1"
  "r19i1p1f1"
  "r20i1p1f1"
  "r21i1p1f1"
  "r22i1p1f1"
  "r23i1p1f1"
  "r24i1p1f1"
  "r25i1p1f1"
  "r26i1p1f1"
  "r27i1p1f1"
  "r28i1p1f1"
  "r29i1p1f1"
  "r30i1p1f1"
)

# =========================
# Time periods
# =========================

historical_periods=(
  "19210101-19301231"
  "19310101-19401231"
  "19410101-19501231"
  "19510101-19601231"
  "19610101-19701231"
  "19710101-19801231"
  "19810101-19901231"
  "19910101-20001231"
  "20010101-20101231"
  "20110101-20141231"
)

scenario_periods=(
  "20150101-20201231"
  "20210101-20301231"
  "20310101-20401231"
)

# =========================
# Start
# =========================

mkdir -p "${outdir}"
cd "${outdir}"

count=0

for sim in "${simulations[@]}"; do

  # Pick correct period list for this simulation
  if [[ "${sim}" == "historical" ]]; then
    periods=("${historical_periods[@]}")
  elif [[ "${sim}" == "scenarioSSP5-85" ]]; then
    periods=("${scenario_periods[@]}")
  else
    echo "Unknown simulation type: ${sim}"
    exit 1
  fi

  for member in "${members[@]}"; do
    for period in "${periods[@]}"; do

      filename="${var}_${freq}_${model}_${sim}_${member}_${grid}_${period}.nc"
      url="${ftp_site}/${sim}/${member}/${freq}/${var}/${grid}/${version}/${filename}"

      # Stop when reaching maximum number of files
      if [[ "${count}" -ge "${MAX_FILES}" ]]; then
        echo "Reached MAX_FILES=${MAX_FILES}. Stopping."
        exit 0
      fi

      # Optionally skip existing files
      if [[ "${SKIP_EXISTING}" == "true" && -f "${filename}" ]]; then
        echo "Skipping existing file: ${filename}"
        continue
      fi

      echo "Downloading (${count}/${MAX_FILES}): ${filename}"
      echo "URL: ${url}"

      # -f: fail on server errors
      # -O: save with remote filename
      # --retry 3: retry a few times
      curl -f -O --retry 3 "${url}"

      count=$((count + 1))
    done
  done
done

echo "Done. Downloaded ${count} files."
