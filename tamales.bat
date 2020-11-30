@echo off
title Actualiza cubos!
echo #####Actualiza cubos de Tamales Inc #####

start /WAIT /b cmd /c "activate tamales & python process.py --noinput"

echo ####Proceso terminado!!!!
