// CI/CD pipeline: unit tests -> PostgreSQL tests -> image tagged with the commit -> push to ECR
// -> deploy that exact commit (deploy.sh rolls back automatically if it isn't healthy).
//
// Jenkins settings (all optional except where noted):
//   ECR_REPOSITORY  <account>.dkr.ecr.<region>.amazonaws.com/ledgerflow (printed by provision_ec2.py);
//                   needs a username/password credential 'aws-ecr-push' = an access key allowed to push
//   DEPLOY_HOST     the server's IP; needs an SSH credential 'deploy-ssh-key'. Only main is deployed.
//   DEPLOY_USER     usually ec2-user
//   SITE_URL        e.g. https://1-2-3-4.sslip.io, checked from outside after the deploy
pipeline {
    agent any

    options {
        disableConcurrentBuilds()  // two deploys must never run at once
    }

    environment {
        IMAGE = "ledgerflow:${env.GIT_COMMIT}"
        TEST_DB = "ledgerflow-test-db-${env.BUILD_NUMBER}"
        DOCKER_BUILDKIT = "1"
    }

    stages {
        stage('Install dependencies') {
            steps {
                sh 'python3 -m venv .venv'
                sh '.venv/bin/pip install --quiet -r requirements-dev.txt'
            }
        }

        stage('Unit tests') {
            steps {
                sh '.venv/bin/pytest -q tests.py --junitxml=test-results.xml'
            }
            post {
                always { junit 'test-results.xml' }
            }
        }

        stage('PostgreSQL tests') {
            steps {
                // A throwaway database on Jenkins' own Docker network, reached by container name.
                sh '''
                    NETWORK=$(docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{$name}}{{end}}' "$(hostname)")
                    docker run -d --rm --name "$TEST_DB" --network "$NETWORK" \
                        -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=test postgres:16-alpine
                    for i in $(seq 1 30); do docker exec "$TEST_DB" pg_isready -U test -d test && break; sleep 1; done
                    sleep 2
                    TEST_DATABASE_URL="postgresql+psycopg2://test:test@$TEST_DB:5432/test" \
                        .venv/bin/pytest -q tests_postgres.py --junitxml=test-results-postgres.xml
                '''
            }
            post {
                always {
                    sh 'docker rm -f "$TEST_DB" || true'
                    junit 'test-results-postgres.xml'
                }
            }
        }

        stage('Build image') {
            steps {
                // The server is ARM (AWS Graviton), so the image is built for arm64.
                sh 'docker build --platform linux/arm64 --provenance=false -t "$IMAGE" .'

            }
        }

        stage('Push image') {
            when { expression { env.ECR_REPOSITORY } }
            steps {
                withCredentials([usernamePassword(credentialsId: 'aws-ecr-push',
                        usernameVariable: 'AWS_ACCESS_KEY_ID', passwordVariable: 'AWS_SECRET_ACCESS_KEY')]) {
                    sh '''
                        .venv/bin/python ci/ecr.py password "$ECR_REPOSITORY" |
                            docker login --username AWS --password-stdin "${ECR_REPOSITORY%%/*}"
                        # Tags are immutable: a rebuilt commit keeps the image that was pushed first.
                        if ! .venv/bin/python ci/ecr.py exists "$ECR_REPOSITORY" "$GIT_COMMIT"; then
                            docker tag "$IMAGE" "$ECR_REPOSITORY:$GIT_COMMIT"
                            docker push "$ECR_REPOSITORY:$GIT_COMMIT"
                        fi
                    '''
                }
            }
        }

        stage('Deploy') {
            when {
                allOf {
                    expression { env.DEPLOY_HOST }
                    expression { !env.BRANCH_NAME || env.BRANCH_NAME == 'main' }
                }
            }
            steps {
                // 'deploy-ssh-key' is a Jenkins credential holding the server's private key.
                sshagent(credentials: ['deploy-ssh-key']) {
                    sh 'ssh -o StrictHostKeyChecking=accept-new "$DEPLOY_USER@$DEPLOY_HOST" "cd LedgerFlow && ./deploy.sh $GIT_COMMIT"'
                }
                script {
                    if (env.SITE_URL) {
                        sh 'curl -fsS --retry 5 --retry-delay 5 "$SITE_URL/health"'
                    }
                }
            }
        }
    }

    post {
        always { sh 'docker image rm "$IMAGE" || true' }
        success { echo "Build ${env.BUILD_NUMBER} (${env.GIT_COMMIT}) passed" }
        failure { echo "Build ${env.BUILD_NUMBER} failed - check the stage logs above" }
    }
}
