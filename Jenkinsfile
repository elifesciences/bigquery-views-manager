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
                lock('bigquery-views-manager--ci') {
                    withBigQueryViewsManagerGcpCredentials {
                        cleanDataset('bigquery_views_manager_ci', commit)
                        try {
                            updateDataset('bigquery_views_manager_ci', commit)
                        } finally {
                            cleanDataset('bigquery_views_manager_ci', commit)
                        }
                    }
                }
            }
        }
    }
}

def cleanDataset(dataset, commit) {
    echo "cleaning dataset: ${dataset}"
    sh "make IMAGE_TAG=${commit} REVISION=${commit} DATASET_NAME=${dataset} ci-example-data-clean-dataset"
}

def updateDataset(dataset, commit) {
    echo "updating dataset: ${dataset}"
    sh "make IMAGE_TAG=${commit} REVISION=${commit} DATASET_NAME=${dataset} ci-example-data-update-dataset"
}

def withBigQueryViewsManagerGcpCredentials(doSomething) {
    try {
        sh 'vault.sh kv get -format json -field credentials secret/containers/bigquery-views-manager/gcp > credentials.json'
        doSomething()
    } finally {
        sh 'echo > credentials.json'
    }
}
