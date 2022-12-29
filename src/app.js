const express = require("express");
const bodyParser = require("body-parser");
const { sequelize, Op, QueryTypes } = require("./model");
const { getProfile } = require("./middleware/getProfile");
const app = express();
app.use(bodyParser.json());
app.set("sequelize", sequelize);
app.set("models", sequelize.models);

/**
 * @returns contract by id
 */
app.get("/contracts/:id", getProfile, async (req, res) => {
  const { id } = req.params;
  const { Contract } = req.app.get("models");
  const contract = await Contract.findOne({
    where: {
      [Op.and]: {
        id,
        [Op.or]: {
          ClientId: req.profile.id,
          ContractorId: req.profile.id,
        },
      },
    },
  });
  if (!contract) return res.status(404).end();
  res.json(contract);
});

/**
 * @returns contracts that belong to the profile
 */
app.get("/contracts", getProfile, async (req, res) => {
  const { Contract } = req.app.get("models");
  const contracts = await Contract.findAll({
    where: {
      [Op.and]: {
        status: { [Op.ne]: "terminated" },
        [Op.or]: {
          ClientId: req.profile.id,
          ContractorId: req.profile.id,
        },
      },
    },
  });
  if (!contracts) return res.status(404).end();
  res.json(contracts);
});

/**
 * @returns unpaid jobs for the profile
 */
app.get("/jobs/unpaid", getProfile, async (req, res) => {
  const { Contract, Job } = req.app.get("models");
  const unpaidJobs = await await Job.findAll(
    {
      include: {
        model: Contract,
        required: true,
        where: {
          [Op.or]: {
            ClientId: req.profile.id,
            ContractorId: req.profile.id,
          },
          status: "in_progress",
        },
        attributes: [],
      },
      where: {
        paid: false,
      },
    },
  );
  if (!unpaidJobs) return res.status(404).end();
  res.json(unpaidJobs);
});

/**
 * @returns pays a job
 */
app.post("/jobs/:job_id/pay", getProfile, async (req, res) => {
  const { Contract, Job, Profile } = req.app.get("models");
  //default isolation level is SERIALIZABLE
  const transaction = await sequelize.transaction();
  try {
    const { job_id } = req.params;
    const client = await Profile.findOne(
      { where: { id: req.get("profile_id") } },
      { transaction }
    );
    const job = await Job.findOne(
      {
        include: {
          model: Contract,
          required: true,
          where: {
            [Op.or]: {
              ClientId: req.profile.id,
            },
          },
          attributes: ["ContractorId"],
        },
        where: {
          id: job_id,
          paid: { [Op.ne]: true },
        },
      },
      { transaction }
    );

    if (!job || job?.price > client?.balance) {
      return res.status(403).end();
    }

    await Profile.update(
      { balance: client.balance - job.price },
      { where: { id: client.id }, transaction }
    );

    await sequelize.query(
      `
        UPDATE Profiles SET balance = balance + :price
        WHERE id = :contractorId
    `,
      {
        replacements: {
          contractorId: job.Contract.ContractorId,
          price: job.price,
        },
        type: QueryTypes.UPDATE,
        model: Profile,
        transaction,
      }
    );

    await Job.update(
      {
        paid: true,
        paymentDate: new Date(),
      },
      { where: { id: job.id }, transaction }
    );

    res.status(200).end();
  } catch (err) {
    await transaction.rollback();
    console.error(err);
    res.status(500).end();
  } finally {
    if (!transaction.finished) {
      await transaction.commit();
    }
  }
});

/**
 * @returns deposits balance in a profile
 */
app.post("/balances/deposit/:userId", getProfile, async (req, res) => {
  const MAX_PERCENTAGE = 0.25;
  const { Contract, Job, Profile } = req.app.get("models");
  //default isolation level is SERIALIZABLE
  const transaction = await sequelize.transaction();
  try {
    const { userId } = req.params;
    const { deposit } = req.query;
    const [client] = await sequelize.query(
      `
        SELECT SUM(price) as totalDebtBalance, Profiles.*
        FROM Profiles
        INNER JOIN Contracts
            ON Profiles.id = Contracts.ClientId
        INNER JOIN Jobs
            ON Contracts.id = Jobs.ContractId
        WHERE NOT Jobs.paid AND Profiles.id = :userId
    `,
      { replacements: { userId }, type: QueryTypes.SELECT, transaction }
    );

    if (client.totalDebtBalance * MAX_PERCENTAGE < deposit) {
      return res.status(403).end();
    }

    await Profile.update(
      { balance: client.balance + Number(deposit) },
      { where: { id: client.id }, transaction }
    );

    res.status(200).end();
  } catch (err) {
    await transaction.rollback();
    console.error(err);
    res.status(500).end();
  } finally {
    if (!transaction.finished) {
      await transaction.commit();
    }
  }
});

/**
 * @returns best profession for the given time period
 */
app.get("/admin/best-profession", getProfile, async (req, res) => {
  const { Profile } = req.app.get("models");
  const { start, end } = req.query;
  try {
    const bestProfession = await sequelize.query(
      `
        SELECT profession
        FROM Profiles
        INNER JOIN Contracts
            ON Profiles.id = Contracts.ContractorId
        INNER JOIN Jobs
            ON Jobs.ContractId = Contracts.id
        WHERE Jobs.paymentDate between :start AND :end
        GROUP BY profession
        ORDER BY sum(Jobs.price) DESC
        LIMIT 1
        `,
      {
        replacements: { start, end },
        type: QueryTypes.SELECT,
        model: Profile,
        mapToModel: true,
      }
    );
    if (!bestProfession) return res.status(404).end();
    res.json(bestProfession);
  } catch (err) {
    console.error(err);
    res.status(500).end();
  }
});

/**
 * @returns best clients for the given time period
 */
app.get("/admin/best-clients", getProfile, async (req, res) => {
  const { Profile } = req.app.get("models");
  const { start, end, limit } = req.query;
  try {
    const bestProfession = await sequelize.query(
      `
        SELECT Profiles.*
        FROM Profiles
        INNER JOIN Contracts
            ON Profiles.id = Contracts.ClientId
        INNER JOIN Jobs
            ON Jobs.ContractId = Contracts.id
        WHERE Jobs.paymentDate between :start AND :end
        GROUP BY Profiles.id
        ORDER BY sum(Jobs.price) DESC
        LIMIT :limit
          `,
      {
        replacements: { start, end, limit: limit ?? 2 },
        type: QueryTypes.SELECT,
        model: Profile,
        mapToModel: true,
      }
    );
    if (!bestProfession) return res.status(404).end();
    res.json(bestProfession);
  } catch (err) {
    console.error(err);
    res.status(500).end();
  }
});

module.exports = app;
