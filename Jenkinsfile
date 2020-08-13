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
        "--dataset=${dataset} create-or-replace-views --materialize  --disable-view-name-mapping",
        commit
    )
}

def withBigQueryViewsManagerGcpCredentials(doSomething) {
    try {
        // temp
        sh 'vault.sh kv list secret/containers/bigquery-views-manager/gcp'
        sh 'vault.sh kv get -field credentials secret/containers/bigquery-views-manager/gcp > credentials.json'
        doSomething()
    } finally {
        sh 'echo > credentials.json'
    }
}
