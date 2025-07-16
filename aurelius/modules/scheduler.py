"""
Scheduler module for AURELIUS backend system.
Manages all scheduled tasks and periodic operations.
"""

import asyncio
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import traceback

from ..logger import get_logger, LogOperation, log_async_function_calls
from ..config import settings
from .analytics import generate_daily_report, generate_weekly_report, generate_monthly_report
from .auto_learning import LearningEngine
from .sales import process_sales_tasks
from .social import twitter, mastodon, discord
from .ai import content_generator
from .data_store import data_store

logger = get_logger("scheduler")

class TaskScheduler:
    """Manages scheduled tasks and periodic operations."""
    
    def __init__(self):
        self.running_tasks: Dict[str, asyncio.Task] = {}
        self.task_configs: Dict[str, Dict[str, Any]] = {}
        self.learning_engine = LearningEngine()
        self.is_running = False
    
    async def start(self):
        """Start the scheduler and all scheduled tasks."""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        self.is_running = True
        logger.info("Starting AURELIUS task scheduler")
        
        try:
            # Initialize task configurations
            await self._initialize_task_configs()
            
            # Start all scheduled tasks
            await self._start_scheduled_tasks()
            
            logger.info("Task scheduler started successfully")
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {str(e)}")
            self.is_running = False
            raise
    
    async def stop(self):
        """Stop the scheduler and cancel all running tasks."""
        if not self.is_running:
            return
        
        logger.info("Stopping AURELIUS task scheduler")
        
        # Cancel all running tasks
        for task_name, task in self.running_tasks.items():
            if not task.done():
                logger.info(f"Cancelling task: {task_name}")
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        self.running_tasks.clear()
        self.is_running = False
        logger.info("Task scheduler stopped")
    
    async def _initialize_task_configs(self):
        """Initialize task configurations."""
        self.task_configs = {
            "content_posting": {
                "interval_minutes": settings.post_interval_minutes,
                "enabled": True,
                "last_run": None,
                "function": self._post_scheduled_content
            },
            "social_engagement": {
                "interval_minutes": 30,  # Check for mentions every 30 minutes
                "enabled": True,
                "last_run": None,
                "function": self._process_social_engagement
            },
            "sales_tasks": {
                "interval_minutes": 60,  # Process sales tasks hourly
                "enabled": True,
                "last_run": None,
                "function": self._process_sales_tasks
            },
            "daily_analytics": {
                "interval_minutes": 1440,  # Daily (24 hours)
                "enabled": True,
                "last_run": None,
                "function": self._generate_daily_analytics,
                "run_at_hour": settings.analytics_report_hour
            },
            "weekly_analytics": {
                "interval_minutes": 10080,  # Weekly (7 days)
                "enabled": True,
                "last_run": None,
                "function": self._generate_weekly_analytics,
                "run_at_hour": settings.analytics_report_hour
            },
            "monthly_analytics": {
                "interval_minutes": 43200,  # Monthly (30 days)
                "enabled": True,
                "last_run": None,
                "function": self._generate_monthly_analytics,
                "run_at_hour": settings.analytics_report_hour
            },
            "auto_learning": {
                "interval_minutes": settings.learning_update_hours * 60,
                "enabled": True,
                "last_run": None,
                "function": self._run_auto_learning
            },
            "system_health_check": {
                "interval_minutes": 15,  # Every 15 minutes
                "enabled": True,
                "last_run": None,
                "function": self._system_health_check
            }
        }
    
    async def _start_scheduled_tasks(self):
        """Start all scheduled tasks."""
        for task_name, config in self.task_configs.items():
            if config.get("enabled", True):
                task = asyncio.create_task(
                    self._run_periodic_task(task_name, config),
                    name=task_name
                )
                self.running_tasks[task_name] = task
                logger.info(f"Started scheduled task: {task_name}")
    
    async def _run_periodic_task(self, task_name: str, config: Dict[str, Any]):
        """Run a periodic task with the specified configuration."""
        interval_minutes = config["interval_minutes"]
        task_function = config["function"]
        run_at_hour = config.get("run_at_hour")
        
        logger.info(f"Starting periodic task '{task_name}' with {interval_minutes} minute interval")
        
        while self.is_running:
            try:
                # Check if we should run based on schedule
                should_run = await self._should_run_task(task_name, config, run_at_hour)
                
                if should_run:
                    logger.info(f"Executing scheduled task: {task_name}")
                    
                    try:
                        await task_function()
                        config["last_run"] = datetime.now().isoformat()
                        logger.info(f"Completed scheduled task: {task_name}")
                        
                        # Store task execution info
                        await self._log_task_execution(task_name, True)
                        
                    except Exception as e:
                        logger.error(f"Error in scheduled task '{task_name}': {str(e)}")
                        logger.error(f"Traceback: {traceback.format_exc()}")
                        
                        # Store task execution error
                        await self._log_task_execution(task_name, False, str(e))
                        
                        # Send error alert to Discord
                        try:
                            await discord.send_system_alert(
                                f"Scheduled task '{task_name}' failed: {str(e)}",
                                "scheduler"
                            )
                        except:
                            pass  # Don't let Discord errors break the scheduler
                
                # Wait for next execution
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                logger.info(f"Periodic task '{task_name}' was cancelled")
                break
            except Exception as e:
                logger.error(f"Unexpected error in periodic task '{task_name}': {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying
    
    async def _should_run_task(self, task_name: str, config: Dict[str, Any], 
                              run_at_hour: Optional[int] = None) -> bool:
        """Determine if a task should run based on its schedule."""
        try:
            interval_minutes = config["interval_minutes"]
            last_run_str = config.get("last_run")
            
            # If never run, run immediately (except for time-specific tasks)
            if not last_run_str:
                if run_at_hour is not None:
                    current_hour = datetime.now().hour
                    return current_hour == run_at_hour
                return True
            
            # Parse last run time
            last_run = datetime.fromisoformat(last_run_str)
            time_since_last_run = datetime.now() - last_run
            
            # Check if enough time has passed
            if time_since_last_run.total_seconds() < (interval_minutes * 60):
                return False
            
            # For tasks with specific run hours, check the hour
            if run_at_hour is not None:
                current_hour = datetime.now().hour
                if current_hour != run_at_hour:
                    return False
                
                # Also check if we already ran today at this hour
                if last_run.date() == datetime.now().date() and last_run.hour == run_at_hour:
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error checking task schedule for '{task_name}': {str(e)}")
            return False
    
    async def _log_task_execution(self, task_name: str, success: bool, error: str = None):
        """Log task execution results."""
        try:
            execution_log = {
                "task_name": task_name,
                "timestamp": datetime.now().isoformat(),
                "success": success,
                "error": error
            }
            
            # Store in task execution history
            history_key = f"task_execution_history:{task_name}"
            history = await data_store.get(history_key) or []
            history.insert(0, execution_log)
            
            # Keep only last 100 executions
            history = history[:100]
            await data_store.set(history_key, history)
            
        except Exception as e:
            logger.error(f"Error logging task execution: {str(e)}")
    
    # Scheduled task functions
    @log_async_function_calls("scheduler")
    async def _post_scheduled_content(self):
        """Post scheduled content to social media platforms."""
        try:
            # Get optimal posting parameters from learning insights
            optimal_schedule = await data_store.get("optimal_posting_schedule") or {}
            recommended_hashtags = await data_store.get("recommended_hashtags") or []
            effective_themes = await data_store.get("effective_content_themes") or {}
            
            # Generate content topics based on themes
            topics = effective_themes.get("top_themes", ["business growth", "social media", "automation"])
            selected_topic = topics[datetime.now().day % len(topics)] if topics else "business automation"
            
            # Generate content for each platform
            platforms = ["twitter", "mastodon", "discord"]
            
            for platform in platforms:
                try:
                    # Generate platform-specific content
                    content = await content_generator.generate_social_post(
                        topic=selected_topic,
                        platform=platform,
                        tone="professional"
                    )
                    
                    if content:
                        # Add recommended hashtags
                        if recommended_hashtags and platform in ["twitter", "mastodon"]:
                            hashtags = " ".join(recommended_hashtags[:3])
                            content = f"{content}\n\n{hashtags}"
                        
                        # Post to platform
                        success = False
                        if platform == "twitter":
                            success = await twitter.post_tweet(content)
                        elif platform == "mastodon":
                            success = await mastodon.post_status(content)
                        elif platform == "discord":
                            success = await discord.post_message(content, "AURELIUS Bot")
                        
                        if success:
                            logger.info(f"Posted scheduled content to {platform}")
                        else:
                            logger.warning(f"Failed to post scheduled content to {platform}")
                    
                    # Small delay between posts
                    await asyncio.sleep(5)
                    
                except Exception as e:
                    logger.error(f"Error posting to {platform}: {str(e)}")
                    continue
            
        except Exception as e:
            logger.error(f"Error in scheduled content posting: {str(e)}")
    
    @log_async_function_calls("scheduler")
    async def _process_social_engagement(self):
        """Process social media engagement (mentions, replies, etc.)."""
        try:
            engagement_results = {
                "twitter": 0,
                "mastodon": 0,
                "discord": 0
            }
            
            # Process Twitter mentions
            try:
                twitter_engagements = await twitter.engage_with_mentions(content_generator)
                engagement_results["twitter"] = twitter_engagements
            except Exception as e:
                logger.error(f"Error processing Twitter engagement: {str(e)}")
            
            # Process Mastodon mentions
            try:
                mastodon_engagements = await mastodon.engage_with_mentions(content_generator)
                engagement_results["mastodon"] = mastodon_engagements
            except Exception as e:
                logger.error(f"Error processing Mastodon engagement: {str(e)}")
            
            # Log engagement summary
            total_engagements = sum(engagement_results.values())
            if total_engagements > 0:
                logger.info(f"Processed {total_engagements} social engagements: {engagement_results}")
            
        except Exception as e:
            logger.error(f"Error in social engagement processing: {str(e)}")
    
    @log_async_function_calls("scheduler")
    async def _process_sales_tasks(self):
        """Process sales-related tasks (follow-ups, lead management, etc.)."""
        try:
            results = await process_sales_tasks()
            
            if results.get("follow_ups_processed", 0) > 0:
                logger.info(f"Processed {results['follow_ups_processed']} sales follow-ups")
            
        except Exception as e:
            logger.error(f"Error processing sales tasks: {str(e)}")
    
    @log_async_function_calls("scheduler")
    async def _generate_daily_analytics(self):
        """Generate and distribute daily analytics report."""
        try:
            report = await generate_daily_report()
            
            if report:
                # Send report to Discord
                await discord.send_analytics_report(report)
                logger.info("Daily analytics report generated and sent")
            
        except Exception as e:
            logger.error(f"Error generating daily analytics: {str(e)}")
    
    @log_async_function_calls("scheduler")
    async def _generate_weekly_analytics(self):
        """Generate and distribute weekly analytics report."""
        try:
            # Only run on Mondays
            if datetime.now().weekday() == 0:  # Monday
                report = await generate_weekly_report()
                
                if report:
                    # Send report to Discord
                    await discord.send_analytics_report(report)
                    logger.info("Weekly analytics report generated and sent")
            
        except Exception as e:
            logger.error(f"Error generating weekly analytics: {str(e)}")
    
    @log_async_function_calls("scheduler")
    async def _generate_monthly_analytics(self):
        """Generate and distribute monthly analytics report."""
        try:
            # Only run on the 1st of the month
            if datetime.now().day == 1:
                report = await generate_monthly_report()
                
                if report:
                    # Send report to Discord
                    await discord.send_analytics_report(report)
                    logger.info("Monthly analytics report generated and sent")
            
        except Exception as e:
            logger.error(f"Error generating monthly analytics: {str(e)}")
    
    @log_async_function_calls("scheduler")
    async def _run_auto_learning(self):
        """Run auto-learning cycle to optimize system performance."""
        try:
            insights = await self.learning_engine.run_learning_cycle()
            
            if insights:
                # Send learning summary to Discord
                recommendations = insights.get("recommendations", [])
                if recommendations:
                    summary = f"Auto-learning completed with {len(recommendations)} optimization recommendations:\n"
                    for i, rec in enumerate(recommendations[:5], 1):
                        summary += f"{i}. {rec.get('recommendation', 'N/A')}\n"
                    
                    await discord.post_announcement("Auto-Learning Update", summary)
                
                logger.info("Auto-learning cycle completed successfully")
            
        except Exception as e:
            logger.error(f"Error in auto-learning cycle: {str(e)}")
    
    @log_async_function_calls("scheduler")
    async def _system_health_check(self):
        """Perform system health checks."""
        try:
            health_status = {
                "timestamp": datetime.now().isoformat(),
                "redis_connection": False,
                "api_endpoints": {},
                "task_status": {},
                "memory_usage": "unknown",
                "overall_status": "unknown"
            }
            
            # Check Redis connection
            try:
                await data_store.set("health_check", datetime.now().isoformat(), expire=300)
                test_value = await data_store.get("health_check")
                health_status["redis_connection"] = test_value is not None
            except Exception as e:
                logger.warning(f"Redis health check failed: {str(e)}")
            
            # Check task status
            for task_name, task in self.running_tasks.items():
                health_status["task_status"][task_name] = {
                    "running": not task.done(),
                    "cancelled": task.cancelled() if task.done() else False
                }
            
            # Determine overall status
            redis_ok = health_status["redis_connection"]
            tasks_ok = all(status["running"] for status in health_status["task_status"].values())
            
            if redis_ok and tasks_ok:
                health_status["overall_status"] = "healthy"
            elif redis_ok or tasks_ok:
                health_status["overall_status"] = "degraded"
            else:
                health_status["overall_status"] = "unhealthy"
            
            # Store health status
            await data_store.set("system_health", health_status, expire=3600)
            
            # Send alert if unhealthy
            if health_status["overall_status"] == "unhealthy":
                await discord.send_system_alert(
                    "System health check failed - multiple components are down",
                    "health_monitor"
                )
            
        except Exception as e:
            logger.error(f"Error in system health check: {str(e)}")
    
    async def get_task_status(self) -> Dict[str, Any]:
        """Get current status of all scheduled tasks."""
        try:
            status = {
                "scheduler_running": self.is_running,
                "tasks": {},
                "last_updated": datetime.now().isoformat()
            }
            
            for task_name, config in self.task_configs.items():
                task = self.running_tasks.get(task_name)
                
                status["tasks"][task_name] = {
                    "enabled": config.get("enabled", False),
                    "interval_minutes": config.get("interval_minutes", 0),
                    "last_run": config.get("last_run"),
                    "running": task is not None and not task.done() if task else False,
                    "next_run": self._calculate_next_run(config)
                }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting task status: {str(e)}")
            return {}
    
    def _calculate_next_run(self, config: Dict[str, Any]) -> Optional[str]:
        """Calculate next run time for a task."""
        try:
            last_run_str = config.get("last_run")
            if not last_run_str:
                return "immediately"
            
            last_run = datetime.fromisoformat(last_run_str)
            interval_minutes = config.get("interval_minutes", 60)
            next_run = last_run + timedelta(minutes=interval_minutes)
            
            return next_run.isoformat()
            
        except Exception as e:
            logger.error(f"Error calculating next run time: {str(e)}")
            return None
    
    async def run_task_manually(self, task_name: str) -> bool:
        """Manually run a specific task."""
        try:
            if task_name not in self.task_configs:
                logger.error(f"Unknown task: {task_name}")
                return False
            
            config = self.task_configs[task_name]
            task_function = config["function"]
            
            logger.info(f"Manually executing task: {task_name}")
            await task_function()
            
            config["last_run"] = datetime.now().isoformat()
            await self._log_task_execution(task_name, True)
            
            logger.info(f"Manual task execution completed: {task_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error in manual task execution '{task_name}': {str(e)}")
            await self._log_task_execution(task_name, False, str(e))
            return False

# Global scheduler instance
task_scheduler = TaskScheduler()

# Convenience functions
async def start_scheduler():
    """Start the task scheduler."""
    await task_scheduler.start()

async def stop_scheduler():
    """Stop the task scheduler."""
    await task_scheduler.stop()

async def get_scheduler_status() -> Dict[str, Any]:
    """Get scheduler status."""
    return await task_scheduler.get_task_status()

async def run_task_manually(task_name: str) -> bool:
    """Run a task manually."""
    return await task_scheduler.run_task_manually(task_name)
