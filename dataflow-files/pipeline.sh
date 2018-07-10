set -e

COMMAND=${1}
PROJECT_ID=${2}
BUCKET_NAME=${3}

function usage
{
    echo "$ pipeline.sh run PROJECT_ID BUCKET_NAME"
}

if [[ -z $@ ]]; then
    usage
    exit 0
fi

case "$COMMAND" in
    run )
        mvn compile exec:java \
        -Dexec.mainClass=com.jtangney.gcpex.dataflow.FileAnalyticsPipeline \
        -Dexec.args="\
        --runner=DataflowRunner \
        --project=${PROJECT_ID} \
        --stagingLocation=gs://${BUCKET_NAME}/staging \
        --tempLocation=gs://${BUCKET_NAME}/temp \
        --fileSource=gs://${BUCKET_NAME}/files/input/*.json \
        --fileSink=gs://${BUCKET_NAME}/files/output/"
        ;;
    * )
        usage
        exit 1
        ;;
esac