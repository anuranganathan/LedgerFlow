// CI/CD pipeline: test -> build Docker image -> deploy.
// The Deploy stage only runs when DEPLOY_HOST (the EC2 server's IP) is set in Jenkins.
pipeline {
    agent any

    environment {
        IMAGE = "ledgerflow:${env.BUILD_NUMBER}"
    }

    stages {
        stage('Install dependencies') {
            steps {
                sh 'python3 -m venv .venv'
                sh '.venv/bin/pip install --quiet -r requirements-dev.txt'
            }
        }

        stage('Run tests') {
            steps {
                sh '.venv/bin/pytest -q tests.py --junitxml=test-results.xml'
            }
            post {
                always {
                    junit 'test-results.xml'
                }
            }
        }

        stage('Build Docker image') {
            steps {
                sh 'docker build -t $IMAGE -t ledgerflow:latest .'
            }
        }

        stage('Deploy') {
            when {
                expression { env.DEPLOY_HOST }
            }
            steps {
                // 'deploy-ssh-key' is a Jenkins credential holding the server's private key.
                sshagent(credentials: ['deploy-ssh-key']) {
                    sh 'ssh -o StrictHostKeyChecking=accept-new $DEPLOY_USER@$DEPLOY_HOST "cd LedgerFlow && ./deploy.sh"'
                }
            }
        }
    }

    post {
        success { echo "Build ${env.BUILD_NUMBER} passed" }
        failure { echo "Build ${env.BUILD_NUMBER} failed - check the stage logs above" }
    }
}
