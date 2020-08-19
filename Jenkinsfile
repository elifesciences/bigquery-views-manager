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
                    withPypiCredentials 'staging', 'testpypi', {
                        sh "make IMAGE_TAG=${commit} COMMIT=${commit} NO_BUILD=y ci-push-testpypi"
                    }
                }
            }
        }

        elifeMainlineOnly {
            stage 'Merge to master', {
                elifeGitMoveToBranch commit, 'master'
            }

            stage 'Push unstable bigquery-views-manager image', {
                def image = DockerImage.elifesciences(this, 'bigquery-views-manager', commit)
                def unstable_image = image.addSuffixAndTag('_unstable', commit)
                unstable_image.tag('latest').push()
                unstable_image.push()
            }
        }

        elifeTagOnly { tag ->
            stage 'Push pypi release', {
                withEnv(["VERSION=${version}"]) {
                    withPypiCredentials 'prod', 'pypi', {
                        sh "make IMAGE_TAG=${commit} NO_BUILD=y ci-push-pypi"
                    }
                }
            }

            stage 'Push release bigquery-views-manager image', {
                def image = DockerImage.elifesciences(this, 'bigquery-views-manager', commit)
                image.tag('latest').push()
                image.tag(version).push()
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
