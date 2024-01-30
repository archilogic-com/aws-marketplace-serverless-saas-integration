const winston = require('winston');
const AWS = require('aws-sdk');


const {SupportSNSArn: TopicArn, SubscriptionCheck} = process.env;
const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
  ],
});


async function sendSubscriptionStatus(newImage, status, isRenewed) {
  const SNS = new AWS.SNS({apiVersion: '2010-03-31', region: 'eu-west-1'});
  const contractValue = JSON.parse(newImage.entitlement).Entitlements[0].Value.IntegerValue;
  const message = JSON.stringify({
    customerId: newImage.customerIdentifier,
    productCode: newImage.productCode,
    contractValue, isSubscribed: status,
    isRenewed
  })
  const params = {
    TopicArn: SubscriptionCheck,
    Subject: "Update subscription",
    Message: message,
  };
  logger.debug('params2', {data: params})
  await SNS.publish(params).promise();
}

exports.dynamodbStreamHandler = async (event, context) => {
  await Promise.all(event.Records.map(async (record) => {
    logger.defaultMeta = {requestId: context.awsRequestId};
    logger.debug('event', {'data': event});
    logger.debug('context', {'data': context});
    const oldImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);
    const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);

    // eslint-disable-next-line no-console
    logger.debug('OldImage', {'data': oldImage});
    logger.debug('NewImage', {'data': newImage});
    /*
      successfully_subscribed is set true:
        - for SaaS Contracts: no email is sent but after receiving the message in the subscription topic
        - for SaaS Subscriptions: after reciving the subscribe-success message in subscription-sqs.js

      subscription_expired is set to true:
        - for SaaS Contracts: after detecting expired entitlement in entitlement-sqs.js
        - for SaaS Subscriptions: after reciving the unsubscribe-success message in subscription-sqs.js
    */
    const grantAccess = newImage.successfully_subscribed === true &&
        (oldImage.successfully_subscribed !== true )

    const revokeAccess = newImage.subscription_expired === true
        && !oldImage.subscription_expired;

    let entitlementUpdated = false;

    if (newImage.entitlement && oldImage.entitlement && (newImage.entitlement !== oldImage.entitlement)) {
      entitlementUpdated = true;
    }
    logger.debug('new image suc_sub', {'data': newImage.successfully_subscribed});
    logger.debug('old image suc_sub', {'data': oldImage.successfully_subscribed});
    logger.debug('grantAccess', {'data': grantAccess});
    logger.debug('revokeAccess:', {'data': revokeAccess});
    logger.debug('entitlementUpdated', {'data': entitlementUpdated});

    if (grantAccess || revokeAccess || entitlementUpdated) {
      let message = '';
      let subject = '';

      let isSubscribed = true
      let isRenewed
      if (grantAccess) {
        subject = 'New AWS Marketplace Subscriber';
        message = `subscribe-success: ${JSON.stringify(newImage)}`;
        isRenewed=true;
      } else if (revokeAccess) {
        subject = 'AWS Marketplace customer end of subscription';
        message = `unsubscribe-success: ${JSON.stringify(newImage)}`;
        isSubscribed = false;
      } else if (entitlementUpdated) {
        subject = 'AWS Marketplace customer change of subscription';
        message = `entitlement-updated: ${JSON.stringify(newImage)}`;
        const oldEntitlement = JSON.parse(newImage.entitlement).Entitlements[0];
        const newEntitlement = JSON.parse(newImage.entitlement).Entitlements[0];
        const newExpirationDate = newEntitlement.ExpirationDate;
        const oldExpirationDate = oldEntitlement.ExpirationDate;
        isRenewed = newExpirationDate !== oldExpirationDate;
      }
      await sendSubscriptionStatus(newImage, isSubscribed, isRenewed);
      const SNSparams = {
        TopicArn,
        Subject: subject,
        Message: message,
      };

      logger.info('Sending notification');
      logger.debug('SNSparams', {'data': SNSparams});
      const SNS = new AWS.SNS({apiVersion: '2010-03-31', region: 'us-east-1'});
      await SNS.publish(SNSparams).promise();
    }
  }));


  return {};
};

