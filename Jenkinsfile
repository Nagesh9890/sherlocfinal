pipeline {
  agent any
  stages {
    stage('Version') {
      steps {
        sh 'python3 --version'
      }
    }
    stage('Build Docker Image') {
      steps {
        sh 'docker build -t myimage:latest .'
      }
    }
  }
}
