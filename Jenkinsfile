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
                updateDataset(
                    'bigquery_views_manager_ci',
                    commit
                )
            }
        }
    }
}

def viewsCli(args, commit) {
    dockerComposeRun(
        "bigquery-views",
        "python -m bigquery_views.cli ${args}",
        commit
    )
}

def updateDataset(dataset, commit) {
    echo "updating dataset: ${dataset}"
    viewsCli(
        "--dataset=${dataset} create-or-replace-views --materialize  --disable-view-name-mapping",
        commit
    )
}
