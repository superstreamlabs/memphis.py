def gitBranch = env.BRANCH_NAME
def gitURL = "git@github.com:Memphisdev/memphis.py.git"
def repoUrlPrefix = "memphisos"

node ("small-ec2-fleet") {
  git credentialsId: 'main-github', url: gitURL, branch: gitBranch
  
  try{
    
   stage('Deploy to pypi') {
     sh 'python3 -m pip install hatch twine'
     sh 'python3 -m hatch build'
     withCredentials([usernamePassword(credentialsId: 'python_sdk', usernameVariable: 'USR', passwordVariable: 'PSW')]) {
     sh 'python -m twine upload -u $USR -p $PSW dist/*'
    }
   }
    
    stage('Checkout to version branch'){
      sh """
	sudo yum-config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo 
        sudo yum install gh -y
      """
      withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
        sh """
	  git reset --hard origin/latest
          GIT_SSH_COMMAND='ssh -i $check' git checkout -B \$(make version)
          GIT_SSH_COMMAND='ssh -i $check' git push --set-upstream origin \$(make version)
	"""
      }
      withCredentials([string(credentialsId: 'gh_token', variable: 'GH_TOKEN')]) {
        sh(script:"""gh release create \$(make version) --generate-notes""", returnStdout: true)
      }
    }


    notifySuccessful()

  } catch (e) {
      currentBuild.result = "FAILED"
      cleanWs()
      notifyFailed()
      throw e
  }
}

def notifySuccessful() {
  emailext (
      subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
      body: """<p>SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
        <p>Check console output at &QUOT;<a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a>&QUOT;</p>""",
      recipientProviders: [[$class: 'DevelopersRecipientProvider']]
    )
}

def notifyFailed() {
  emailext (
      subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
      body: """<p>FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
        <p>Check console output at &QUOT;<a href='${env.BUILD_URL}'>${env.JOB_NAME} [${env.BUILD_NUMBER}]</a>&QUOT;</p>""",
      recipientProviders: [[$class: 'DevelopersRecipientProvider']]
    )
}
