pipeline {
    agent any

    environment {
        COMPOSE_PROJECT_NAME = 'tp1-integration-deploiement-continu'

        PYTHON = 'python3'
        RUN_INTEGRATION_TESTS = 'true'
        RUN_E2E_TESTS = 'true'
        PIPELINE_TIMEOUT_SECONDS = '180'
        KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
        POSTGRES_HOST = 'postgres'
        POSTGRES_DB = 'sales'
        POSTGRES_USER = 'sales'
        POSTGRES_PASSWORD = 'sales'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Environment') {
            steps {
                sh 'python3 --version'
                sh 'docker --version'
                sh 'docker-compose --version'
            }
        }

        stage('Install') {
            steps {
                sh 'python3 -m pip install --break-system-packages -r requirements.txt'
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                    python3 -m pytest tests/unit \
                      --cov=app \
                      --cov-report=xml:coverage.xml \
                      --cov-report=term-missing \
                      --junitxml=test-results-unit.xml
                '''
            }
        }

        stage('Integration Tests') {
        steps {
            sh '''
            echo "Cleaning previous TP containers..."
            docker rm -f \
                sales-api \
                sales-kafka \
                sales-kafka-init \
                sales-zookeeper \
                sales-postgres \
                sales-spark-master \
                sales-spark-worker \
                sales-spark-streaming \
                2>/dev/null || true

            docker-compose up -d kafka postgres sales-api spark-master spark-worker

            echo "Waiting for Kafka..."
            until docker exec sales-kafka \
                kafka-topics --bootstrap-server localhost:29092 --list >/dev/null 2>&1
            do
                sleep 2
            done

            echo "Creating Kafka topic..."
            docker exec sales-kafka \
                kafka-topics \
                --bootstrap-server localhost:29092 \
                --create \
                --if-not-exists \
                --topic sales.orders \
                --partitions 1 \
                --replication-factor 1

            docker-compose stop spark-streaming
            docker-compose rm -f spark-streaming
            docker-compose up -d spark-streaming

            echo "Waiting for Spark Streaming to be ready..."
            until docker logs sales-spark-streaming 2>&1 | grep -q "Initial offsets"
            do
                sleep 2
            done

            echo "Spark Streaming is ready."

            python3 -m pytest tests/integration -v \
                --junitxml=test-results-integration.xml
        '''
        }
    }

        stage('Build') {
            steps {
                sh 'docker-compose build sales-api spark-streaming'
            }
        }

        stage('E2E Tests') {
            steps {
                sh '''
                    python3 -m pytest tests/e2e -v \
                      --junitxml=test-results-e2e.xml
                '''
            }
        }

            stage('Cleanup before SonarQube') {
        steps {
            sh '''
                docker stop \
                    sales-api \
                    sales-kafka \
                    sales-zookeeper \
                    sales-spark-master \
                    sales-spark-worker \
                    sales-spark-streaming \
                    sales-postgres \
                    2>/dev/null || true
            '''
        }
    }

    stage('SonarQube') {
        steps {
            script {
                 def scannerHome = tool 'SonarScanner'

                withSonarQubeEnv('SonarQube') {
                        withCredentials([string(credentialsId: 'sonarqube-token', variable: 'SONAR_TOKEN')]) {
                            sh """
                                ${scannerHome}/bin/sonar-scanner \
                                -Dsonar.projectKey=real-time-sales-devops \
                                -Dsonar.projectName="Real-Time Sales DevOps TP" \
                                -Dsonar.token=\$SONAR_TOKEN
                            """
                        }
                    }
                }
            }
        }

        stage('Quality Gate') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
    }

    post {
        always {
            archiveArtifacts allowEmptyArchive: true,
                              artifacts: 'coverage.xml,test-results-*.xml'
        }
    }
}