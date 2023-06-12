pipeline {
  agent any
  stages {
    stage('version') {
      steps {
        sh 'python3 --version'
      }
    }
    stage('Install Dependencies') {
      steps {
        sh 'pip3 install -r requirements.txt'
      }
    }
    stage('hello') {
      steps {
        sh 'python3 app.py'
      }
    }
  }
}
