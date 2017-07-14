import json

class SNSNotification:

    def __init__(self, event):
        if type(event) is str:
            event = json.loads(event)

        record = event["Records"][0]

        self.EventSource = record["EventSource"]
        self.EventVersion = record["EventVersion"]
        self.EventSubscriptionArn = record["EventSubscriptionArn"]

        sns = record.get("Sns")
        if sns:
            self.Type = sns.get("Type")
            self.MessageId = sns.get("MessageId")
            self.TopicArn = sns.get("TopicArn")
            self.Subject = sns.get("Subject")
            self.Timestamp = sns.get("Timestamp")
            self.SignatureVersion = sns.get("SignatureVersion")
            self.SigningCertUrl = sns.get("SigningCertUrl")
            self.UnsubscribeUrl = sns.get("UnsubscribeUrl")
            self.MessageAttributes = sns.get("MessageAttributes")

            self.Message = json.loads(sns["Message"])

            self.AlarmName = self.Message.get("AlarmName")
            self.AlarmDescription = self.Message.get("AlarmDescription")
            self.AWSAccountId = self.Message.get("AWSAccountId")
            self.NewStateValue = self.Message.get("NewStateValue")
            self.NewStateReason = self.Message.get("NewStateReason")
            self.StateChangeTime = self.Message.get("StateChangeTime")
            self.Region = self.Message.get("Region")
            self.OldStateValue = self.Message.get("OldStateValue")
            self.Trigger = self.Message.get("Trigger")

            self.MetricName = self.Trigger.get("MetricName")
            self.Namespace = self.Trigger.get("Namespace")
            self.StatisticType = self.Trigger.get("StatisticType")
            self.Dimensions = self.Trigger.get("Dimensions", [])
            self.Statistic = self.Trigger.get("Statistic")
            self.Unit = self.Trigger.get("Unit")
            self.Period = self.Trigger.get("Period")
            self.EvaluationPeriods = self.Trigger.get("EvaluationPeriods")
            self.ComparisonOperator = self.Trigger.get("ComparisonOperator")
            self.Threshold = self.Trigger.get("Threshold")
            self.TreatMissingData = self.Trigger.get("TreatMissingData")
            self.EvaluateLowSampleCountPercentile = self.Trigger.get("EvaluateLowSampleCountPercentile")

            if self.Namespace == "AWS/SQS":
                for dim in self.Dimensions:
                    if dim["name"] == "QueueName":
                        self.QueueName = dim["value"]
                        break

            m = re.match(r".*?\(([0-9.]+)\)", self.NewStateReason)
            if m:
                self.QueueSize = float(m.groups()[0])
