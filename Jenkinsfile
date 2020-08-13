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

            stage 'Push package to test.pypi.org', {
                withPypiCredentials 'staging', 'testpypi', {
                    sh "make IMAGE_TAG=${commit} COMMIT=${commit} NO_BUILD=y ci-push-testpypi"
                }
            }
        }

        elifeTagOnly { tag ->
            stage 'Push release', {
                withPypiCredentials 'prod', 'pypi', {
                    sh "make IMAGE_TAG=${commit} VERSION=${version} NO_BUILD=y ci-push-pypi"
                }
            }
        }
    }
}


import groovy.json.JsonSlurper

@NonCPS
def jsonToPypirc(String jsonText, String sectionName) {
    def credentials = new JsonSlurper().parseText(jsonText)
    echo "Username: ${credentials.username}"
    return "[${sectionName}]\nusername: ${credentials.username}\npassword: ${credentials.password}"
}

def withPypiCredentials(String env, String sectionName, doSomething) {
    try {
        writeFile(file: '.pypirc', text: jsonToPypirc(sh(
            script: "vault.sh kv get -format=json secret/containers/pypi/${env} | jq .data.data",
            returnStdout: true
        ).trim(), sectionName))
        doSomething()
    } finally {
        sh 'echo > .pypirc'
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
