arreter un pod : kubectl scale deployment/email-sender -n logguardian --replicas=0
relancer : kubectl scale deployment/email-sender -n logguardian --replicas=1
