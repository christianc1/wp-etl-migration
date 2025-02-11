<?php
/**
 * TransformCommand class file
 *
 * Handles transformation of extracted data and outputs the transformed state.
 *
 * @package TenupETL\Commands
 */

namespace TenupETL\Commands;

use TenupETL\Classes\Pipeline\{Pipeline, PipelineJob, PipelineJobType};
use TenupETL\Classes\Config\JobConfig;
use TenupETL\Classes\Load\LedgerRegistry;
use Flow\ETL\Filesystem\SaveMode;
use WP_CLI;

use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\JSON\to_json;

/**
 * Manages ETL data transformation operations.
 *
 * ## EXAMPLES
 *
 *     # Transform data from a job to JSON
 *     $ wp etl transform articles --mode=json --file=output.json
 *
 *     # Transform data to CSV
 *     $ wp etl transform articles --mode=csv --file=output.csv
 */
class TransformCommand extends BaseCommand {

	/**
	 * Transforms data from a configured job source
	 *
	 * ## OPTIONS
	 *
	 * <job>
	 * : Name of the job to transform data from
	 *
	 * [--mode=<mode>]
	 * : Output format (json or csv)
	 * ---
	 * default: json
	 * options:
	 *   - json
	 *   - csv
	 * ---
	 *
	 * [--file=<file>]
	 * : Output file path
	 *
	 * ## EXAMPLES
	 *
	 *     wp etl transform articles --mode=json --file=articles.transformed.json
	 *     wp etl transform media --mode=csv --file=media.transformed.csv
	 *
	 * @param array $args Positional arguments
	 * @param array $assoc_args Associative arguments
	 */
	public function __invoke( $args, $assoc_args ) {
		list( $job_name ) = $args;

		$mode = \WP_CLI\Utils\get_flag_value( $assoc_args, 'mode', 'json' );
		$file = \WP_CLI\Utils\get_flag_value( $assoc_args, 'file', null );

		if ( empty( $file ) ) {
			WP_CLI::error( 'Please specify an output file with --file' );
		}

		try {
			// Get job config
			$job_config = $this->get_job_config( $job_name );

			if ( empty( $job_config['extract'] ) ) {
				WP_CLI::error( "No extract configuration found for job: {$job_name}" );
			}

			if ( empty( $job_config['transform'] ) ) {
				WP_CLI::warning( "No transform configuration found for job: {$job_name}. Output will be raw extracted data." );
			}

			// Create pipeline job
			$job = new PipelineJob(
				$this->config,
				new JobConfig( $job_config ),
				new LedgerRegistry( $this->config )
			);

			// Build and run extract and transform steps
			$job->build();

			WP_CLI::log( "Extracting data from {$job_name}..." );
			$job->process( PipelineJobType::Extract );

			if ( ! empty( $job_config['transform'] ) ) {
				WP_CLI::log( "Transforming data from {$job_name}..." );
				$job->process( PipelineJobType::Transform );
			}

			$destination = join(
				DIRECTORY_SEPARATOR,
				[
					untrailingslashit( TENUP_ETL_PLUGIN_DIR ),
					untrailingslashit( ltrim( $file, './' ) ),
				]
			);

			// Write output
			$writer = 'json' === $mode
				? to_json( $destination )->withFlags( JSON_PRETTY_PRINT | JSON_INVALID_UTF8_SUBSTITUTE )
				: to_csv( $destination );

			$report = $job->get_current_state()
				->mode( SaveMode::Overwrite )
				->write( $writer )
				->run( analyze: true );

			WP_CLI::success(
				sprintf(
					'Transformed %d rows to %s',
					$report->statistics()->totalRows(),
					$destination
				)
			);

		} catch ( \Exception $e ) {
			WP_CLI::error( $e->getMessage() );
		}
	}

	/**
	 * Get job configuration by name
	 *
	 * @param string $job_name Name of the job
	 * @return array Job configuration
	 */
	protected function get_job_config( $job_name ) {
		$migration_config = $this->config->get_value( 'migration' );

		foreach ( $migration_config as $job ) {
			if ( $job['name'] === $job_name ) {
				return $job;
			}
		}

		WP_CLI::error( "Job not found: {$job_name}" );
	}
}
