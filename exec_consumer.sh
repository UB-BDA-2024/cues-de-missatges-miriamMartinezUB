path= pwd
export PYTHONPATH=$PYTHONPATH:$path
echo $PYTHONPATH
python ./consumer/main.py

#poner que se ejecute dentro del docker o a mano... va a ser que a mano quizas dentro del fichero de test o en el main