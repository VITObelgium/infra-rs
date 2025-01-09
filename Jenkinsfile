pipeline {
    agent {
        dockerfile {
            filename "Dockerfile"
            dir "docker"
            additionalBuildArgs '--build-arg USER_ID=$(id -u) --build-arg GROUP_ID=$(id -g)'
            args '-v /home/jenkins/.cache/vcpkg:/home/jenkins/.cache/vcpkg'
        }
    }

    environment {
        //VCPKG_DOWNLOADS = '/home/jenkins/.cache/vcpkg/downloads'
        //VCPKG_DEFAULT_BINARY_CACHE = '/home/jenkins/.cache/vcpkg'
        VCPKG_FORCE_SYSTEM_BINARIES = '1'
    }

    options {
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '30', artifactNumToKeepStr: '10'))
    }

    stages {
        stage('Clean the target directory') {
            steps {
                dir("${env.WORKSPACE}/target") {
                    deleteDir()
                }
            }
        }

        stage('Bootstrap dependencies') {
            steps {
                script {
                    echo "Bootstrap"
                    sh 'curl -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash'
                    sh 'cargo binstall -y just fd-find sd cargo-vcpkg cargo-nextest'
                    sh 'curl -fsSL https://pixi.sh/install.sh | bash'
                    sh 'just bootstrap'
                }
            }
        }

        stage('Build') {
            matrix {
                axes {
                    axis {
                        name 'BUILD_CONFIG'
                        values 'debug', 'release'
                    }
                }
                stages {
                    stage('Build') {
                        steps {
                            script {
                                echo "Build '${BUILD_CONFIG}'"
                                sh 'just build_${BUILD_CONFIG}'
                            }
                        }
                    }

                    stage('Test') {
                        steps {
                            script {
                                echo "Test '${BUILD_CONFIG}'"
                                sh 'just test_${BUILD_CONFIG}'
                            }
                        }
                    }

                    stage('Test python environment') {
                        steps {
                            script {
                                echo "Python test '${BUILD_CONFIG}'"
                                sh 'just test_${BUILD_CONFIG}_py'
                            }
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            junit 'target/nextest/ci/junit.xml'
        }

        failure {
            mail to: "${['dirk.vandenboer@vito.be', params.COMMITTER_EMAIL].unique().join(',')}",
                subject: "${currentBuild.fullDisplayName} build failed",
                body: "See ${env.BUILD_URL} for details."
        }
    }
}
