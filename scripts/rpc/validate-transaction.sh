#!/bin/bash

echo -n "TRANSACTION DATA: "
read TRANSACTION_DATA

echo -n "ENABLE SLP VALIDATION: [Y/n] "
read ENABLE_SLP_VALIDATION

if [[ "${ENABLE_SLP_VALIDATION}" == "y" || "${ENABLE_SLP_VALIDATION}" == "Y" ]]; then
    ENABLE_SLP_VALIDATION=1
else
    ENABLE_SLP_VALIDATION=0
fi

(echo "{\"method\":\"GET\",\"query\":\"VALIDATE_TRANSACTION\",\"parameters\":{\"hash\":\"${TRANSACTION_DATA}\",\"enableSlpValidation\":\"${ENABLE_SLP_VALIDATION}\"}}}") | nc localhost 8334

