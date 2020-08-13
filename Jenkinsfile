elifePipeline {
    def commit

    node('containers-jenkins-plugin') {

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
        }

        stage 'Build and run tests', {
            try {
                sh "make IMAGE_TAG=${commit} REVISION=${commit} ci-build-and-test"
            } finally {
                sh "make ci-clean"
            }
        }

        elifePullRequestOnly { prNumber ->
            stage 'Create and delete views', {
                withBigQueryViewsManagerGcpCredentials {
                    updateDataset(
                        'bigquery_views_manager_ci',
                        commit
                    )
                }
            }
        }
    }
}

def viewsCli(args, commit) {
    sh "make IMAGE_TAG=${commit} REVISION=${commit} ci-views-manager-cli ARGS='${args}'"
}

def updateDataset(dataset, commit) {
    echo "updating dataset: ${dataset}"
    viewsCli(
        "--dataset=${dataset} create-or-replace-views \
        --view-list-file=/tmp/example-data/views/views.lst \
        --materialized-view-list-file=/tmp/example-data/views/materialized-views.lst \
        --materialize \
        --disable-view-name-mapping",
        commit
    )
}

def withBigQueryViewsManagerGcpCredentials(doSomething) {
    try {
        sh 'rm -rf credentials.json || true'
        sh 'vault.sh kv get -format json -field credentials secret/containers/bigquery-views-manager/gcp > credentials.json'
        sh 'head -c50 credentials.json'
        doSomething()
    } finally {
        sh 'echo > credentials.json'
    }
}
