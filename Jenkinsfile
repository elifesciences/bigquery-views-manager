elifePipeline {
    node('containers-jenkins-plugin') {
        def commit
        def version

        stage 'Checkout', {
            checkout scm
            commit = elifeGitRevision()
            if (env.TAG_NAME) {
                version = env.TAG_NAME - 'v'
            } else {
                version = 'develop'
            }
        }

        stage 'Build and run tests', {
            withEnv(["VERSION=${version}"]) {
                try {
                    sh "make IMAGE_TAG=${commit} REVISION=${commit} ci-build-and-test"
                } finally {
                    sh "make ci-clean"
                }
            }
        }

        elifePullRequestOnly { prNumber ->
            stage 'Create and delete views', {
                lock('bigquery-views-manager--ci') {
                    withEnv(["VERSION=${version}"]) {
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

            stage 'Push package to test.pypi.org', {
                withEnv(["VERSION=${version}"]) {
                    withPypiCredentials 'test', 'testpypi', {
                        sh "make IMAGE_TAG=${commit} COMMIT=${commit} NO_BUILD=y ci-push-testpypi"
                    }
                }
            }
        }

        elifeTagOnly { tag ->
            stage 'Push pypi release', {
                withEnv(["VERSION=${version}"]) {
                    withPypiCredentials 'live', 'pypi', {
                        sh "make IMAGE_TAG=${commit} NO_BUILD=y ci-push-pypi"
                    }
                }
            }
        }
    }
}


def withPypiCredentials(String env, String sectionName, doSomething) {
    withCredentials([string(credentialsId: "pypi-credentials--${env}", variable: 'TWINE_PASSWORD')]) {
        doSomething()
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
        // remove potential credentials.json directory in case it was created by the mount
        sh 'rm -rf credentials.json || true'
        sh 'vault.sh kv get -format json -field credentials secret/containers/bigquery-views-manager/gcp > credentials.json'
        doSomething()
    } finally {
        sh 'echo > credentials.json'
    }
}
