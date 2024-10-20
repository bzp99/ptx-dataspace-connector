import axios from 'axios';
import { Logger } from '../../../libs/loggers';
import { handle } from '../../../libs/loaders/handler';
import {
    DataExchange,
    IData,
    IDataExchange,
    IParams,
} from '../../../utils/types/dataExchange';
import { getEndpoint } from '../../../libs/loaders/configuration';
import { getCatalogData } from '../../../libs/services/catalog';
import { ExchangeError } from '../../../libs/errors/exchangeError';
import { getContract } from '../../../libs/services/contract';

export const triggerBilateralFlow = async (props: {
    contract: string;
    resources: string[] | IData[];
    providerParams?: IParams;
}) => {
    const { resources, providerParams } = props;

    const contract = props.contract;

    // retrieve contract
    const [contractResponse] = await handle(getContract(contract));
    // get Provider endpoint
    const [providerResponse] = await handle(
        axios.get(contractResponse.dataProvider)
    );

    const [resourceResponse] = await handle(
        axios.get(contractResponse.serviceOffering)
    );

    if (!providerResponse?.dataspaceEndpoint) {
        Logger.error({
            message: 'Provider missing PDC endpoint',
            location: 'consumerExchange',
        });
        throw new ExchangeError(
            'Provider missing PDC endpoint',
            'triggerBilateralFlow',
            500
        );
    }

    const mappedResources = resourcesMapper({
        resources,
        resourceResponse,
        serviceOffering: contractResponse.serviceOffering,
    });

    let dataExchange: IDataExchange;

    if (providerResponse?.dataspaceEndpoint !== (await getEndpoint())) {
        dataExchange = await DataExchange.create({
            providerEndpoint: providerResponse?.dataspaceEndpoint,
            resources: mappedResources,
            purposeId: contractResponse.purpose[0].purpose,
            contract: props.contract,
            status: 'PENDING',
            providerParams: providerParams ?? [],
            createdAt: new Date(),
        });
        // Create the data exchange at the provider
        await dataExchange.createDataExchangeToOtherParticipant('provider');
    } else {
        const [consumerResponse] = await handle(
            axios.get(contractResponse.dataConsumer)
        );
        dataExchange = await DataExchange.create({
            consumerEndpoint: consumerResponse?.dataspaceEndpoint,
            resources: mappedResources,
            purposeId: contractResponse.purpose[0].purpose,
            contract: props.contract,
            status: 'PENDING',
            providerParams: providerParams ?? [],
            createdAt: new Date(),
        });
        // Create the data exchange at the provider
        await dataExchange.createDataExchangeToOtherParticipant('consumer');
    }

    return {
        dataExchange,
        providerEndpoint: providerResponse?.dataspaceEndpoint,
    };
};

export const triggerEcosystemFlow = async (props: {
    resourceId: string;
    purposeId: string;
    contract: string;
    resources: string[] | IData[];
    providerParams?: IParams;
}) => {
    const { resourceId, purposeId, contract, resources, providerParams } =
        props;

    // retrieve contract
    const [contractResponse] = await handle(getContract(contract));

    //Create a data Exchange
    let dataExchange: IDataExchange;

    // verify providerEndpoint, resource and purpose exists
    if (!resourceId && !purposeId) {
        Logger.error({
            message: 'Missing body params',
            location: 'consumerExchange',
        });
        throw new ExchangeError(
            'Missing body params',
            'triggerEcosystemFlow',
            500
        );
    }

    //check if resource and purpose exists inside contract
    const resource = contractResponse.serviceOfferings.find(
        (so: { serviceOffering: string }) => so.serviceOffering === resourceId
    );
    const purpose = contractResponse.serviceOfferings.find(
        (so: { serviceOffering: string }) => so.serviceOffering === purposeId
    );

    if (!purpose) {
        Logger.error({
            message: 'Wrong purpose given',
            location: 'consumerExchange',
        });
        throw new ExchangeError(
            'Wrong purpose given',
            'triggerEcosystemFlow',
            500
        );
    }
    if (!resource) {
        Logger.error({
            message: 'Wrong resource given',
            location: 'consumerExchange',
        });
        throw new ExchangeError(
            'Wrong resource given',
            'triggerEcosystemFlow',
            500
        );
    }

    const [serviceOfferingResponse] = await handle(getCatalogData(resourceId));

    const mappedResources = resourcesMapper({
        resources,
        resourceResponse: serviceOfferingResponse,
        serviceOffering: resourceId,
    });

    const consumerEndpoint = purpose.participant;
    const providerEndpoint = resource.participant;

    if (consumerEndpoint === (await getEndpoint())) {
        //search consumerEndpoint
        dataExchange = await DataExchange.create({
            providerEndpoint: providerEndpoint,
            resources: mappedResources,
            purposeId: purposeId,
            contract: contract,
            status: 'PENDING',
            providerParams: providerParams,
            createdAt: new Date(),
        });
        await dataExchange.createDataExchangeToOtherParticipant('provider');
    } else if (providerEndpoint === (await getEndpoint())) {
        dataExchange = await DataExchange.create({
            consumerEndpoint: consumerEndpoint,
            resources: mappedResources,
            purposeId: purposeId,
            contract: contract,
            status: 'PENDING',
            providerParams: providerParams ?? [],
            createdAt: new Date(),
        });

        // Create the data exchange at the provider
        await dataExchange.createDataExchangeToOtherParticipant('consumer');
    }

    return {
        dataExchange,
        providerEndpoint: providerEndpoint,
    };
};

const resourcesMapper = (props: {
    resources: string[] | IData[];
    resourceResponse: any;
    serviceOffering: string;
}) => {
    const { resources, resourceResponse, serviceOffering } = props;

    let mappedResources:
        | (
              | { serviceOffering: any; resource: string }
              | {
                    serviceOffering: any;
                    resource: string;
                    params: [IParams];
                }
          )[]
        | undefined;

    if (!resources || resources?.length === 0) {
        mappedResources = resourceResponse.dataResources.map(
            (dt: string | IData) => {
                if (typeof dt === 'string') {
                    return {
                        serviceOffering: serviceOffering,
                        resource: dt,
                    };
                } else {
                    return {
                        serviceOffering: serviceOffering,
                        resource: dt.resource,
                        params: dt.params,
                    };
                }
            }
        );
    } else {
        mappedResources = resources?.map((dt: string | IData) => {
            if (typeof dt === 'string') {
                const resourceExists = resourceResponse.dataResources.find(
                    (so: string) => so === dt
                );
                if (resourceExists) {
                    return {
                        serviceOffering: serviceOffering,
                        resource: dt,
                    };
                } else {
                    throw new Error(
                        "resource doesn't exists in the service offering"
                    );
                }
            } else {
                const resourceExists = resourceResponse.dataResources.find(
                    (so: string) => so === dt.resource
                );
                if (resourceExists) {
                    return {
                        serviceOffering: serviceOffering,
                        resource: dt.resource,
                        params: dt.params,
                    };
                } else {
                    throw new Error(
                        "resource doesn't exists in the service offering"
                    );
                }
            }
        });
    }

    return mappedResources;
};
