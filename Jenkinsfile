pipeline {
    agent any

    options {
        buildDiscarder(logRotator(numToKeepStr: '10'))
        disableConcurrentBuilds()
        timeout(time: 45, unit: 'MINUTES')
    }

    stages {
        stage('Build') {
            steps { sh 'scripts/build.sh' }
        }
        stage('Test') {
            steps { sh 'scripts/test.sh' }
        }
    }
}
